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

    def lrange(self, key, start, end):
        values = self._lists.get(key, [])
        if end == -1:
            end = len(values) - 1
        return values[start:end + 1]

    def delete(self, key):
        self._lists.pop(key, None)
        self._sets.pop(key, None)


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

        socket_events._registered = False
        socket_events._online_users.clear()

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
        with self.app.app_context():
            from app.models.block_model import Block

            self.db.session.query(Block).delete()
            self.db.session.commit()
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


    def test_attachment_message_delivered_with_type(self):
        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)

        alice.get_received()
        bob.get_received()

        attachment = {
            "type": "image",
            "url": "https://cdn.example.com/messages/a.png",
            "mime_type": "image/png",
            "file_name": "a.png",
        }
        alice.emit(
            "send_message",
            {
                "to": "bob",
                "message": None,
                "encrypted_key": "file-key-1",
                "type": "image",
                "attachment": attachment,
            },
        )

        alice_events = alice.get_received()
        bob_events = bob.get_received()

        sent_events = [event for event in alice_events if event["name"] == "message_sent"]
        self.assertEqual(len(sent_events), 1)
        self.assertEqual(sent_events[0]["args"][0]["type"], "image")

        new_message_events = [event for event in bob_events if event["name"] == "new_message"]
        self.assertEqual(len(new_message_events), 1)
        payload = new_message_events[0]["args"][0]
        self.assertEqual(payload["type"], "image")
        self.assertEqual(payload["attachment"]["url"], attachment["url"])

    def test_user_status_events_and_query(self):
        alice = self._connect(self.alice_token)
        alice.get_received()

        bob = self._connect(self.bob_token)
        alice_events = alice.get_received()
        status_events = [event for event in alice_events if event["name"] == "user_status"]
        self.assertTrue(any(e["args"][0]["username"] == "bob" and e["args"][0]["online"] for e in status_events))

        alice.emit("get_user_status", {"username": "bob"})
        queried = alice.get_received()
        query_events = [event for event in queried if event["name"] == "user_status"]
        self.assertTrue(any(e["args"][0]["username"] == "bob" and e["args"][0]["online"] for e in query_events))

        bob.disconnect()
        post_disconnect_events = alice.get_received()
        offline_events = [event for event in post_disconnect_events if event["name"] == "user_status"]
        self.assertTrue(any(e["args"][0]["username"] == "bob" and not e["args"][0]["online"] for e in offline_events))

    def test_blocked_user_cannot_send_message_and_receives_block_event(self):
        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)

        alice.get_received()
        bob.get_received()

        client = self.app.test_client()
        block_response = client.post(
            "/api/blocks/bob",
            headers={"Authorization": f"Bearer {self.alice_token}"},
        )
        self.assertEqual(block_response.status_code, 201)

        bob_events = bob.get_received()
        blocked_events = [event for event in bob_events if event["name"] == "chat_blocked"]
        self.assertEqual(len(blocked_events), 1)
        blocked_payload = blocked_events[0]["args"][0]
        self.assertEqual(blocked_payload["blocked_by"], "alice")
        self.assertEqual(blocked_payload["chat_id"], "alice")

        bob.emit(
            "send_message",
            {"to": "alice", "message": "enc-msg-blocked", "encrypted_key": "key-blocked"},
        )
        bob_send_events = bob.get_received()
        message_errors = [event for event in bob_send_events if event["name"] == "message_error"]
        self.assertEqual(len(message_errors), 1)
        error_payload = message_errors[0]["args"][0]
        self.assertEqual(error_payload["code"], "blocked")

        alice_events = alice.get_received()
        self.assertFalse(any(event["name"] == "new_message" for event in alice_events))


if __name__ == "__main__":
    unittest.main()
