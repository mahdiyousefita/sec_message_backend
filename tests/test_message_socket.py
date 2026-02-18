import os
import tempfile
import unittest
from unittest.mock import patch


class FakeRedis:
    def __init__(self):
        self._sets = {}
        self._lists = {}

    def clear(self):
        self._sets.clear()
        self._lists.clear()

    def sadd(self, key, value):
        self._sets.setdefault(key, set()).add(value)

    def smembers(self, key):
        return self._sets.get(key, set())

    def sismember(self, key, value):
        return value in self._sets.get(key, set())

    def rpush(self, key, value):
        self._lists.setdefault(key, []).append(value)

    def lpop(self, key):
        values = self._lists.get(key, [])
        if not values:
            return None
        return values.pop(0)


class TestSocketMessageFlow(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db_fd, cls.db_path = tempfile.mkstemp(suffix=".db")
        os.close(db_fd)
        os.environ["DATABASE_URL"] = f"sqlite:///{cls.db_path}"
        os.environ["JWT_SECRET_KEY"] = "test-secret"

        from app import create_app
        from app.db import db
        from app.extensions.extensions import socketio
        from app.services import auth_service, message_service
        from app.repositories import message_repository
        from app.extensions import redis_client as redis_module
        import app.routes.contact_routes as contact_routes
        import app.socket_events as socket_events

        cls.app = create_app()
        cls.db = db
        cls.socketio = socketio
        cls.auth_service = auth_service
        cls.message_service = message_service
        cls.socket_events = socket_events

        cls.fake_redis = FakeRedis()
        cls.redis_patches = [
            patch.object(message_repository, "redis_client", cls.fake_redis),
            patch.object(redis_module, "redis_client", cls.fake_redis),
            patch.object(contact_routes, "r", cls.fake_redis),
        ]
        for patcher in cls.redis_patches:
            patcher.start()

        with cls.app.app_context():
            cls.db.drop_all()
            cls.db.create_all()
            cls.auth_service.register("alice", "pass123", "alice_pub_key")
            cls.auth_service.register("bob", "pass123", "bob_pub_key")
            cls.alice_token = cls.auth_service.login("alice", "pass123")["access_token"]
            cls.bob_token = cls.auth_service.login("bob", "pass123")["access_token"]

    @classmethod
    def tearDownClass(cls):
        for patcher in cls.redis_patches:
            patcher.stop()

        if os.path.exists(cls.db_path):
            os.remove(cls.db_path)

    def setUp(self):
        self.fake_redis.clear()
        self.socket_events._online_users.clear()
        self.clients = []

    def tearDown(self):
        for client in self.clients:
            if client.is_connected():
                client.disconnect()
        self.socket_events._online_users.clear()

    def _connect(self, token):
        client = self.socketio.test_client(
            self.app,
            flask_test_client=self.app.test_client(),
            auth={"token": token},
        )
        self.clients.append(client)
        self.assertTrue(client.is_connected())
        return client

    def test_online_recipient_receives_live_message(self):
        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)

        alice.get_received()
        bob.get_received()

        alice.emit(
            "send_message",
            {"to": "bob", "message": "enc-msg-1", "encrypted_key": "key-1"},
        )

        alice_events = alice.get_received()
        bob_events = bob.get_received()

        sent_events = [event for event in alice_events if event["name"] == "message_sent"]
        self.assertEqual(len(sent_events), 1)

        new_message_events = [event for event in bob_events if event["name"] == "new_message"]
        self.assertEqual(len(new_message_events), 1)
        payload = new_message_events[0]["args"][0]

        self.assertEqual(payload["from"], "alice")
        self.assertEqual(payload["message"], "enc-msg-1")
        self.assertEqual(payload["encrypted_key"], "key-1")

        self.assertEqual(self.message_service.receive_messages("bob"), [])

    def test_offline_recipient_gets_pending_messages_on_connect(self):
        alice = self._connect(self.alice_token)
        alice.get_received()

        alice.emit(
            "send_message",
            {"to": "bob", "message": "enc-msg-2", "encrypted_key": "key-2"},
        )
        alice_events = alice.get_received()

        sent_events = [event for event in alice_events if event["name"] == "message_sent"]
        self.assertEqual(len(sent_events), 1)

        bob = self._connect(self.bob_token)
        bob_events = bob.get_received()

        pending_events = [
            event for event in bob_events if event["name"] == "pending_messages"
        ]
        self.assertEqual(len(pending_events), 1)

        messages = pending_events[0]["args"][0]["messages"]
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]["from"], "alice")
        self.assertEqual(messages[0]["message"], "enc-msg-2")
        self.assertEqual(messages[0]["encrypted_key"], "key-2")

        self.assertEqual(self.message_service.receive_messages("bob"), [])


if __name__ == "__main__":
    unittest.main()
