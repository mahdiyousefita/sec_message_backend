import os
import tempfile
import unittest
from unittest.mock import patch

from tests.fake_redis import FakeRedis


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
        if hasattr(socket_events, "_presence_state_lock"):
            with socket_events._presence_state_lock:
                socket_events._user_sids.clear()
                socket_events._sid_group_rooms.clear()
        else:
            socket_events._user_sids.clear()
            socket_events._sid_group_rooms.clear()

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
            cls.auth_service.register("carol", "pass123", "carol_pub_key")
            cls.alice_token = cls.auth_service.login("alice", "pass123")["access_token"]
            cls.bob_token = cls.auth_service.login("bob", "pass123")["access_token"]
            cls.carol_token = cls.auth_service.login("carol", "pass123")["access_token"]

    @classmethod
    def tearDownClass(cls):
        for patcher in cls.redis_patches:
            patcher.stop()

        if os.path.exists(cls.db_path):
            os.remove(cls.db_path)

    def setUp(self):
        self.fake_redis.clear()
        if hasattr(self.socket_events, "_presence_state_lock"):
            with self.socket_events._presence_state_lock:
                self.socket_events._user_sids.clear()
                self.socket_events._sid_group_rooms.clear()
        else:
            self.socket_events._user_sids.clear()
            self.socket_events._sid_group_rooms.clear()
        self._client_message_counter = 0
        with self.app.app_context():
            from app.models.block_model import Block
            from app.models.chat_message_model import (
                GroupMessage,
                GroupMessageKeyRecipient,
                GroupMessageRecipient,
                GroupMessageUserDelete,
                PrivateMessage,
                PrivateMessageUserDelete,
            )
            from app.models.group_model import Group, GroupMember

            self.db.session.query(GroupMessageUserDelete).delete()
            self.db.session.query(PrivateMessageUserDelete).delete()
            self.db.session.query(GroupMessageRecipient).delete()
            self.db.session.query(GroupMessageKeyRecipient).delete()
            self.db.session.query(GroupMessage).delete()
            self.db.session.query(PrivateMessage).delete()
            self.db.session.query(GroupMember).delete()
            self.db.session.query(Group).delete()
            self.db.session.query(Block).delete()
            self.db.session.commit()
        self.clients = []

    def tearDown(self):
        for client in self.clients:
            if client.is_connected():
                client.disconnect()
        if hasattr(self.socket_events, "_presence_state_lock"):
            with self.socket_events._presence_state_lock:
                self.socket_events._user_sids.clear()
                self.socket_events._sid_group_rooms.clear()
        else:
            self.socket_events._user_sids.clear()
            self.socket_events._sid_group_rooms.clear()

    def _connect(self, token):
        client = self.socketio.test_client(
            self.app,
            flask_test_client=self.app.test_client(),
            auth={"token": token},
        )
        self.clients.append(client)
        self.assertTrue(client.is_connected())
        return client

    def _auth_headers(self, token):
        return {"Authorization": f"Bearer {token}"}

    def _next_client_message_id(self, prefix):
        self._client_message_counter += 1
        return f"{prefix}-{self._client_message_counter}"

    def _emit_private_message(self, client, payload):
        request_payload = dict(payload)
        request_payload.setdefault(
            "client_message_id",
            self._next_client_message_id("client-private"),
        )
        client.emit("send_message", request_payload)
        return request_payload

    def _emit_group_message(self, client, payload):
        request_payload = dict(payload)
        request_payload.setdefault(
            "client_message_id",
            self._next_client_message_id("client-group"),
        )
        client.emit("send_group_message", request_payload)
        return request_payload

    def _presence_token_key_for_user(self, username):
        if hasattr(self.socket_events, "_presence_state_lock"):
            with self.socket_events._presence_state_lock:
                user_sids = set(self.socket_events._user_sids.get(username, set()))
        else:
            user_sids = set(self.socket_events._user_sids.get(username, set()))
        self.assertTrue(user_sids)
        sid = next(iter(user_sids))
        token = self.socket_events._presence_connection_token(sid)
        return self.socket_events._presence_connection_token_key(token)

    def _create_group(self, name="group-alpha"):
        with self.app.app_context():
            from app.models.group_model import Group, GroupMember
            from app.models.user_model import User

            creator = User.query.filter_by(username="alice").first()
            member = User.query.filter_by(username="bob").first()
            self.assertIsNotNone(creator)
            self.assertIsNotNone(member)

            group = Group(name=name, creator_id=creator.id)
            self.db.session.add(group)
            self.db.session.flush()
            self.db.session.add(GroupMember(group_id=group.id, user_id=creator.id))
            self.db.session.add(GroupMember(group_id=group.id, user_id=member.id))
            self.db.session.commit()
            return group.id

    def test_online_recipient_receives_live_message(self):
        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)

        alice.get_received()
        bob.get_received()

        self._emit_private_message(
            alice,
            {"to": "bob", "message": "enc-msg-1", "encrypted_key": "key-1"},
        )

        alice_events = alice.get_received()
        bob_events = bob.get_received()

        sent_events = [event for event in alice_events if event["name"] == "message_sent"]
        self.assertEqual(len(sent_events), 1)

        new_message_events = [event for event in bob_events if event["name"] == "new_message"]
        self.assertEqual(len(new_message_events), 1)
        payload = new_message_events[0]["args"][0]
        private_notification_events = [
            event for event in bob_events if event["name"] == "new_notification"
        ]
        self.assertEqual(len(private_notification_events), 0)

        self.assertEqual(payload["from"], "alice")
        self.assertEqual(payload["message"], "enc-msg-1")
        self.assertEqual(payload["encrypted_key"], "key-1")

        self.assertEqual(self.message_service.receive_messages("bob"), [])

    def test_private_message_sent_ack_echoes_client_message_id(self):
        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)

        alice.get_received()
        bob.get_received()

        self._emit_private_message(
            alice,
            {
                "to": "bob",
                "message": "enc-msg-correlation",
                "encrypted_key": "key-correlation",
                "client_message_id": "client-private-123",
            },
        )

        alice_events = alice.get_received()
        bob_events = bob.get_received()

        sent_events = [event for event in alice_events if event["name"] == "message_sent"]
        self.assertEqual(len(sent_events), 1)
        self.assertEqual(
            sent_events[0]["args"][0].get("client_message_id"),
            "client-private-123",
        )

        live_events = [event for event in bob_events if event["name"] == "new_message"]
        self.assertEqual(len(live_events), 1)
        self.assertEqual(
            live_events[0]["args"][0].get("client_message_id"),
            "client-private-123",
        )

    def test_private_send_rejects_missing_client_message_id(self):
        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)
        alice.get_received()
        bob.get_received()

        alice.emit(
            "send_message",
            {
                "to": "bob",
                "message": "enc-msg-missing-client-id",
                "encrypted_key": "key-missing-client-id",
            },
        )

        alice_events = alice.get_received()
        bob_events = bob.get_received()

        error_events = [event for event in alice_events if event["name"] == "message_error"]
        self.assertEqual(len(error_events), 1)
        self.assertEqual(
            error_events[0]["args"][0].get("code"),
            "invalid_client_message_id",
        )
        self.assertIn("required", error_events[0]["args"][0].get("error", "").lower())

        sent_events = [event for event in alice_events if event["name"] == "message_sent"]
        self.assertEqual(len(sent_events), 0)
        live_events = [event for event in bob_events if event["name"] == "new_message"]
        self.assertEqual(len(live_events), 0)

    def test_private_retry_with_same_client_message_id_is_idempotent(self):
        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)

        alice.get_received()
        bob.get_received()

        send_payload = {
            "to": "bob",
            "message": "enc-msg-idempotent",
            "encrypted_key": "key-idempotent",
            "client_message_id": "client-private-idempotent-1",
        }
        self._emit_private_message(alice, send_payload)

        first_alice_events = alice.get_received()
        first_bob_events = bob.get_received()

        first_sent_events = [
            event for event in first_alice_events if event["name"] == "message_sent"
        ]
        first_live_events = [
            event for event in first_bob_events if event["name"] == "new_message"
        ]
        self.assertEqual(len(first_sent_events), 1)
        self.assertEqual(len(first_live_events), 1)

        first_sent_payload = first_sent_events[0]["args"][0]
        first_message_id = first_sent_payload["message_id"]
        first_timestamp = first_sent_payload["timestamp"]

        self._emit_private_message(alice, send_payload)
        second_alice_events = alice.get_received()
        second_bob_events = bob.get_received()

        second_sent_events = [
            event for event in second_alice_events if event["name"] == "message_sent"
        ]
        second_live_events = [
            event for event in second_bob_events if event["name"] == "new_message"
        ]
        self.assertEqual(len(second_sent_events), 1)
        self.assertEqual(len(second_live_events), 0)
        self.assertEqual(second_sent_events[0]["args"][0]["message_id"], first_message_id)
        self.assertEqual(second_sent_events[0]["args"][0]["timestamp"], first_timestamp)

        with self.app.app_context():
            from app.models.chat_message_model import PrivateMessage

            rows = PrivateMessage.query.filter_by(
                sender_username="alice",
                recipient_username="bob",
                client_message_id="client-private-idempotent-1",
            ).all()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].message_id, first_message_id)

    def test_contacts_updated_emits_incremental_payload(self):
        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)

        alice.get_received()
        bob.get_received()

        self._emit_private_message(
            alice,
            {"to": "bob", "message": "enc-delta-1", "encrypted_key": "key-delta-1"},
        )

        alice_events = alice.get_received()
        bob_events = bob.get_received()

        sender_contacts_events = [
            event for event in alice_events if event["name"] == "contacts_updated"
        ]
        recipient_contacts_events = [
            event for event in bob_events if event["name"] == "contacts_updated"
        ]

        self.assertEqual(len(sender_contacts_events), 1)
        self.assertEqual(len(recipient_contacts_events), 1)

        sender_payload = sender_contacts_events[0]["args"][0]
        recipient_payload = recipient_contacts_events[0]["args"][0]

        self.assertEqual(sender_payload.get("contact"), "bob")
        self.assertEqual(sender_payload.get("sync_reason"), "outgoing_message")
        self.assertIn("has_unread", sender_payload)
        self.assertIn("unread_count", sender_payload)

        self.assertEqual(recipient_payload.get("contact"), "alice")
        self.assertEqual(recipient_payload.get("sync_reason"), "incoming_message")
        self.assertIn("has_unread", recipient_payload)
        self.assertIn("unread_count", recipient_payload)

    def test_offline_recipient_gets_pending_messages_on_connect(self):
        alice = self._connect(self.alice_token)
        alice.get_received()

        self._emit_private_message(
            alice,
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

    def test_private_pending_survives_transient_redis_reset(self):
        alice = self._connect(self.alice_token)
        alice.get_received()

        self._emit_private_message(
            alice,
            {"to": "bob", "message": "enc-durable", "encrypted_key": "key-durable"},
        )
        sent_events = [event for event in alice.get_received() if event["name"] == "message_sent"]
        self.assertEqual(len(sent_events), 1)
        message_id = sent_events[0]["args"][0]["message_id"]

        self.fake_redis.clear()

        bob = self._connect(self.bob_token)
        bob_events = bob.get_received()
        pending_events = [event for event in bob_events if event["name"] == "pending_messages"]
        self.assertEqual(len(pending_events), 1)
        pending_messages = pending_events[0]["args"][0]["messages"]
        self.assertEqual(len(pending_messages), 1)
        self.assertEqual(pending_messages[0]["message_id"], message_id)

    def test_group_pending_survives_transient_redis_reset(self):
        group_id = self._create_group(name="durable-group")

        alice = self._connect(self.alice_token)
        alice.get_received()

        self._emit_group_message(
            alice,
            {
                "group_id": group_id,
                "message": "enc-group-durable",
                "encrypted_keys": {
                    "alice": "alice-key",
                    "bob": "bob-key",
                },
            },
        )
        sent_events = [event for event in alice.get_received() if event["name"] == "group_message_sent"]
        self.assertEqual(len(sent_events), 1)
        message_id = sent_events[0]["args"][0]["message_id"]

        self.fake_redis.clear()

        bob = self._connect(self.bob_token)
        bob_events = bob.get_received()
        pending_events = [event for event in bob_events if event["name"] == "pending_group_messages"]
        self.assertEqual(len(pending_events), 1)
        payload = pending_events[0]["args"][0]
        self.assertEqual(payload["group_id"], group_id)
        self.assertEqual(len(payload["messages"]), 1)
        self.assertEqual(payload["messages"][0]["message_id"], message_id)
        self.assertEqual(payload["messages"][0]["encrypted_keys"], {"bob": "bob-key"})
        self.assertEqual(payload["messages"][0]["encrypted_key"], "bob-key")

    def test_group_live_delivery_scopes_encrypted_key_per_recipient(self):
        group_id = self._create_group(name="recipient-scoped-group")

        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)
        alice.get_received()
        bob.get_received()

        self._emit_group_message(
            alice,
            {
                "group_id": group_id,
                "message": "enc-group-live",
                "encrypted_keys": {
                    "alice": "alice-live-key",
                    "bob": "bob-live-key",
                },
            },
        )

        alice_events = alice.get_received()
        bob_events = bob.get_received()

        alice_group_events = [
            event for event in alice_events if event["name"] == "new_group_message"
        ]
        bob_group_events = [
            event for event in bob_events if event["name"] == "new_group_message"
        ]
        self.assertEqual(len(alice_group_events), 1)
        self.assertEqual(len(bob_group_events), 1)

        alice_payload = alice_group_events[0]["args"][0]
        bob_payload = bob_group_events[0]["args"][0]

        self.assertEqual(alice_payload["encrypted_keys"], {"alice": "alice-live-key"})
        self.assertEqual(alice_payload["encrypted_key"], "alice-live-key")
        self.assertEqual(bob_payload["encrypted_keys"], {"bob": "bob-live-key"})
        self.assertEqual(bob_payload["encrypted_key"], "bob-live-key")

        sent_events = [
            event for event in alice_events if event["name"] == "group_message_sent"
        ]
        self.assertEqual(len(sent_events), 1)

    def test_group_message_sent_ack_echoes_client_message_id(self):
        group_id = self._create_group(name="group-correlation")

        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)
        alice.get_received()
        bob.get_received()

        self._emit_group_message(
            alice,
            {
                "group_id": group_id,
                "message": "enc-group-correlation",
                "encrypted_keys": {
                    "alice": "alice-key",
                    "bob": "bob-key",
                },
                "client_message_id": "client-group-123",
            },
        )

        alice_events = alice.get_received()
        bob_events = bob.get_received()

        sent_events = [
            event for event in alice_events if event["name"] == "group_message_sent"
        ]
        self.assertEqual(len(sent_events), 1)
        self.assertEqual(
            sent_events[0]["args"][0].get("client_message_id"),
            "client-group-123",
        )

        group_events = [
            event for event in bob_events if event["name"] == "new_group_message"
        ]
        self.assertEqual(len(group_events), 1)
        self.assertEqual(
            group_events[0]["args"][0].get("client_message_id"),
            "client-group-123",
        )

    def test_group_send_accepts_recipient_key_records_contract(self):
        group_id = self._create_group(name="group-key-records")

        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)
        alice.get_received()
        bob.get_received()

        self._emit_group_message(
            alice,
            {
                "group_id": group_id,
                "message": "enc-group-records",
                "group_key_ref": "sender-key-ref-1",
                "recipient_key_records": [
                    {"recipient": "alice", "encrypted_key": "alice-key-record"},
                    {"recipient": "bob", "encrypted_key": "bob-key-record"},
                ],
            },
        )

        alice_events = alice.get_received()
        bob_events = bob.get_received()

        sent_events = [
            event for event in alice_events if event["name"] == "group_message_sent"
        ]
        self.assertEqual(len(sent_events), 1)

        bob_group_events = [
            event for event in bob_events if event["name"] == "new_group_message"
        ]
        self.assertEqual(len(bob_group_events), 1)
        bob_payload = bob_group_events[0]["args"][0]
        self.assertEqual(bob_payload.get("encrypted_key"), "bob-key-record")
        self.assertEqual(bob_payload.get("encrypted_keys"), {"bob": "bob-key-record"})

    def test_group_send_can_reuse_group_key_ref_without_reuploading_records(self):
        group_id = self._create_group(name="group-key-ref-reuse")

        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)
        alice.get_received()
        bob.get_received()

        shared_key_ref = "sender-key-ref-reuse"
        self._emit_group_message(
            alice,
            {
                "group_id": group_id,
                "message": "enc-first",
                "group_key_ref": shared_key_ref,
                "recipient_key_records": [
                    {"recipient": "alice", "encrypted_key": "alice-key-reuse"},
                    {"recipient": "bob", "encrypted_key": "bob-key-reuse"},
                ],
                "client_message_id": "client-group-reuse-1",
            },
        )
        first_alice_events = alice.get_received()
        first_bob_events = bob.get_received()
        self.assertEqual(
            len([event for event in first_alice_events if event["name"] == "group_message_sent"]),
            1,
        )
        first_bob_group_events = [
            event for event in first_bob_events if event["name"] == "new_group_message"
        ]
        self.assertEqual(len(first_bob_group_events), 1)
        self.assertEqual(
            first_bob_group_events[0]["args"][0].get("encrypted_key"),
            "bob-key-reuse",
        )

        self._emit_group_message(
            alice,
            {
                "group_id": group_id,
                "message": "enc-second",
                "group_key_ref": shared_key_ref,
                "client_message_id": "client-group-reuse-2",
            },
        )
        second_alice_events = alice.get_received()
        second_bob_events = bob.get_received()

        second_sent_events = [
            event for event in second_alice_events if event["name"] == "group_message_sent"
        ]
        self.assertEqual(len(second_sent_events), 1)
        second_bob_group_events = [
            event for event in second_bob_events if event["name"] == "new_group_message"
        ]
        self.assertEqual(len(second_bob_group_events), 1)
        self.assertEqual(
            second_bob_group_events[0]["args"][0].get("encrypted_key"),
            "bob-key-reuse",
        )

    def test_group_send_rejects_missing_client_message_id(self):
        group_id = self._create_group(name="group-missing-client-id")
        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)
        alice.get_received()
        bob.get_received()

        alice.emit(
            "send_group_message",
            {
                "group_id": group_id,
                "message": "enc-group-missing-client-id",
                "encrypted_keys": {
                    "alice": "alice-key",
                    "bob": "bob-key",
                },
            },
        )

        alice_events = alice.get_received()
        bob_events = bob.get_received()

        error_events = [event for event in alice_events if event["name"] == "message_error"]
        self.assertEqual(len(error_events), 1)
        self.assertEqual(
            error_events[0]["args"][0].get("code"),
            "invalid_client_message_id",
        )
        self.assertIn("required", error_events[0]["args"][0].get("error", "").lower())

        sent_events = [event for event in alice_events if event["name"] == "group_message_sent"]
        self.assertEqual(len(sent_events), 0)
        group_events = [
            event for event in bob_events if event["name"] == "new_group_message"
        ]
        self.assertEqual(len(group_events), 0)

    def test_group_retry_with_same_client_message_id_is_idempotent(self):
        group_id = self._create_group(name="group-idempotent")

        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)
        alice.get_received()
        bob.get_received()

        send_payload = {
            "group_id": group_id,
            "message": "enc-group-idempotent",
            "encrypted_keys": {
                "alice": "alice-key",
                "bob": "bob-key",
            },
            "client_message_id": "client-group-idempotent-1",
        }
        self._emit_group_message(alice, send_payload)

        first_alice_events = alice.get_received()
        first_bob_events = bob.get_received()

        first_sent_events = [
            event for event in first_alice_events if event["name"] == "group_message_sent"
        ]
        first_bob_group_events = [
            event for event in first_bob_events if event["name"] == "new_group_message"
        ]
        self.assertEqual(len(first_sent_events), 1)
        self.assertEqual(len(first_bob_group_events), 1)

        first_sent_payload = first_sent_events[0]["args"][0]
        first_message_id = first_sent_payload["message_id"]
        first_timestamp = first_sent_payload["timestamp"]

        self._emit_group_message(alice, send_payload)
        second_alice_events = alice.get_received()
        second_bob_events = bob.get_received()

        second_sent_events = [
            event for event in second_alice_events if event["name"] == "group_message_sent"
        ]
        second_bob_group_events = [
            event for event in second_bob_events if event["name"] == "new_group_message"
        ]
        self.assertEqual(len(second_sent_events), 1)
        self.assertEqual(len(second_bob_group_events), 0)
        self.assertEqual(second_sent_events[0]["args"][0]["message_id"], first_message_id)
        self.assertEqual(second_sent_events[0]["args"][0]["timestamp"], first_timestamp)

        with self.app.app_context():
            from app.models.chat_message_model import GroupMessage

            rows = GroupMessage.query.filter_by(
                sender_username="alice",
                group_id=group_id,
                client_message_id="client-group-idempotent-1",
            ).all()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0].message_id, first_message_id)

    def test_group_ack_marks_delivery_without_marking_seen(self):
        group_id = self._create_group(name="group-delivery-read-separation")

        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)
        alice.get_received()
        bob.get_received()

        self._emit_group_message(
            alice,
            {
                "group_id": group_id,
                "message": "enc-group-delivery",
                "encrypted_keys": {
                    "alice": "alice-key",
                    "bob": "bob-key",
                },
            },
        )

        alice_events = alice.get_received()
        bob_events = bob.get_received()
        sent_events = [event for event in alice_events if event["name"] == "group_message_sent"]
        self.assertEqual(len(sent_events), 1)
        message_id = sent_events[0]["args"][0]["message_id"]

        incoming_events = [event for event in bob_events if event["name"] == "new_group_message"]
        self.assertEqual(len(incoming_events), 1)
        self.assertEqual(incoming_events[0]["args"][0]["message_id"], message_id)

        bob.emit(
            "ack_group_messages",
            {"group_id": group_id, "message_ids": [message_id]},
        )
        ack_events = [event for event in bob.get_received() if event["name"] == "ack_group_confirmed"]
        self.assertEqual(len(ack_events), 1)
        self.assertEqual(ack_events[0]["args"][0]["removed"], 1)

        alice_after_ack = alice.get_received()
        delivered_events = [
            event for event in alice_after_ack if event["name"] == "group_message_delivered"
        ]
        seen_events = [
            event for event in alice_after_ack if event["name"] == "group_message_seen"
        ]
        self.assertEqual(len(delivered_events), 1)
        self.assertEqual(delivered_events[0]["args"][0]["message_id"], message_id)
        self.assertEqual(delivered_events[0]["args"][0]["delivered_to"], "bob")
        self.assertEqual(len(seen_events), 0)

        alice.emit(
            "get_group_delivered_messages",
            {"group_id": group_id, "message_ids": [message_id]},
        )
        delivered_status_events = [
            event for event in alice.get_received() if event["name"] == "group_delivered_messages_status"
        ]
        self.assertEqual(len(delivered_status_events), 1)
        self.assertIn(message_id, delivered_status_events[0]["args"][0]["message_ids"])

        alice.emit(
            "get_group_seen_messages",
            {"group_id": group_id, "message_ids": [message_id]},
        )
        seen_status_events = [
            event for event in alice.get_received() if event["name"] == "group_seen_messages_status"
        ]
        self.assertEqual(len(seen_status_events), 1)
        self.assertEqual(seen_status_events[0]["args"][0]["message_ids"], [])

    def test_group_mark_read_emits_seen_after_delivery_ack(self):
        group_id = self._create_group(name="group-read-explicit")

        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)
        alice.get_received()
        bob.get_received()

        self._emit_group_message(
            alice,
            {
                "group_id": group_id,
                "message": "enc-group-read",
                "encrypted_keys": {
                    "alice": "alice-key",
                    "bob": "bob-key",
                },
            },
        )

        alice_events = alice.get_received()
        bob_events = bob.get_received()
        sent_events = [event for event in alice_events if event["name"] == "group_message_sent"]
        self.assertEqual(len(sent_events), 1)
        message_id = sent_events[0]["args"][0]["message_id"]
        self.assertTrue(any(event["name"] == "new_group_message" for event in bob_events))

        bob.emit(
            "ack_group_messages",
            {"group_id": group_id, "message_ids": [message_id]},
        )
        bob.get_received()
        alice.get_received()

        bob.emit(
            "mark_read_group_messages",
            {"group_id": group_id, "message_ids": [message_id]},
        )
        read_events = [event for event in bob.get_received() if event["name"] == "group_read_confirmed"]
        self.assertEqual(len(read_events), 1)
        self.assertEqual(read_events[0]["args"][0]["marked"], 1)

        alice_after_read = alice.get_received()
        seen_events = [event for event in alice_after_read if event["name"] == "group_message_seen"]
        self.assertEqual(len(seen_events), 1)
        self.assertEqual(seen_events[0]["args"][0]["message_id"], message_id)
        self.assertEqual(seen_events[0]["args"][0]["seen_by"], "bob")

        alice.emit(
            "get_group_seen_messages",
            {"group_id": group_id, "message_ids": [message_id]},
        )
        seen_status_events = [
            event for event in alice.get_received() if event["name"] == "group_seen_messages_status"
        ]
        self.assertEqual(len(seen_status_events), 1)
        self.assertIn(message_id, seen_status_events[0]["args"][0]["message_ids"])

    def test_removed_member_stops_receiving_group_realtime_events(self):
        group_id = self._create_group(name="membership-safe-group")

        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)
        alice.get_received()
        bob.get_received()

        bob.emit("join_group", {"group_id": group_id})
        bob.get_received()

        remove_response = self.app.test_client().delete(
            f"/api/groups/{group_id}/members/bob",
            headers=self._auth_headers(self.alice_token),
        )
        self.assertEqual(remove_response.status_code, 200)

        bob_after_removal = bob.get_received()
        revoked_events = [
            event for event in bob_after_removal if event["name"] == "group_membership_revoked"
        ]
        self.assertEqual(len(revoked_events), 1)
        self.assertEqual(revoked_events[0]["args"][0]["group_id"], group_id)

        bob.emit(
            "ack_group_messages",
            {"group_id": group_id, "message_ids": ["non-existent-message-id"]},
        )
        membership_errors = [
            event
            for event in bob.get_received()
            if event["name"] == "message_error"
            and "not a member" in event["args"][0].get("error", "").lower()
        ]
        self.assertGreaterEqual(len(membership_errors), 1)

        self._emit_group_message(
            alice,
            {
                "group_id": group_id,
                "message": "enc-after-removal",
                "encrypted_keys": {"alice": "alice-key"},
            },
        )
        alice_events = alice.get_received()
        sent_events = [
            event for event in alice_events if event["name"] == "group_message_sent"
        ]
        self.assertEqual(len(sent_events), 1)
        message_id = sent_events[0]["args"][0]["message_id"]

        bob_realtime_after_send = bob.get_received()
        bob_new_group_events = [
            event for event in bob_realtime_after_send if event["name"] == "new_group_message"
        ]
        self.assertEqual(len(bob_new_group_events), 0)

        alice.emit(
            "delete_group_message",
            {"group_id": group_id, "message_id": message_id},
        )
        alice.get_received()
        bob_after_delete = bob.get_received()
        leaked_delete_events = [
            event for event in bob_after_delete if event["name"] == "group_message_deleted"
        ]
        self.assertEqual(len(leaked_delete_events), 0)

    def test_group_delivery_revalidates_members_on_membership_version_change(self):
        group_id = self._create_group(name="membership-version-race-group")

        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)
        alice.get_received()
        bob.get_received()

        remove_response = self.app.test_client().delete(
            f"/api/groups/{group_id}/members/bob",
            headers=self._auth_headers(self.alice_token),
        )
        self.assertEqual(remove_response.status_code, 200)
        bob.get_received()

        with self.app.app_context():
            from app.repositories import group_repository as group_repo

            real_get_member_usernames = group_repo.get_group_member_usernames

        version_sequence = iter([1, 2, 2, 2, 2, 2])
        member_call_count = {"value": 0}

        def fake_membership_version(_group_id):
            try:
                return next(version_sequence)
            except StopIteration:
                return 2

        def fake_member_usernames(_group_id):
            member_call_count["value"] += 1
            if member_call_count["value"] == 1:
                return ["alice", "bob"]
            return real_get_member_usernames(_group_id)

        with patch(
            "app.services.group_delivery_guard.group_repository.get_membership_version",
            side_effect=fake_membership_version,
        ):
            with patch(
                "app.services.group_delivery_guard.group_repository.get_group_member_usernames",
                side_effect=fake_member_usernames,
            ):
                self._emit_group_message(
                    alice,
                    {
                        "group_id": group_id,
                        "message": "enc-race-safe",
                        "encrypted_keys": {
                            "alice": "alice-key",
                            "bob": "bob-key",
                        },
                    },
                )

        alice_events = alice.get_received()
        sent_events = [event for event in alice_events if event["name"] == "group_message_sent"]
        self.assertEqual(len(sent_events), 1)
        self.assertGreaterEqual(member_call_count["value"], 2)

        bob_events = bob.get_received()
        leaked_messages = [event for event in bob_events if event["name"] == "new_group_message"]
        self.assertEqual(len(leaked_messages), 0)

    def test_readded_member_requires_rejoin_for_group_realtime_delivery(self):
        group_id = self._create_group(name="group-rejoin-required")

        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)
        alice.get_received()
        bob.get_received()

        remove_response = self.app.test_client().delete(
            f"/api/groups/{group_id}/members/bob",
            headers=self._auth_headers(self.alice_token),
        )
        self.assertEqual(remove_response.status_code, 200)
        bob.get_received()

        with self.app.app_context():
            from app.repositories import group_repository, user_repository

            bob_user = user_repository.get_by_username("bob")
            self.assertIsNotNone(bob_user)
            readded = group_repository.add_member(group_id, bob_user.id)
            self.assertTrue(readded)
            group_repository.bump_membership_version(group_id)

        self._emit_group_message(
            alice,
            {
                "group_id": group_id,
                "message": "enc-before-rejoin",
                "encrypted_keys": {
                    "alice": "alice-key-1",
                    "bob": "bob-key-1",
                },
            },
        )
        alice.get_received()
        bob_before_join = bob.get_received()
        before_join_events = [
            event for event in bob_before_join if event["name"] == "new_group_message"
        ]
        self.assertEqual(len(before_join_events), 0)

        bob.emit("join_group", {"group_id": group_id})
        joined_events = [event for event in bob.get_received() if event["name"] == "group_joined"]
        self.assertEqual(len(joined_events), 1)
        self.assertEqual(joined_events[0]["args"][0]["group_id"], group_id)

        self._emit_group_message(
            alice,
            {
                "group_id": group_id,
                "message": "enc-after-rejoin",
                "encrypted_keys": {
                    "alice": "alice-key-2",
                    "bob": "bob-key-2",
                },
            },
        )
        alice.get_received()
        bob_after_join = bob.get_received()
        after_join_events = [
            event for event in bob_after_join if event["name"] == "new_group_message"
        ]
        self.assertEqual(len(after_join_events), 1)

    def test_join_group_syncs_pending_messages_as_seen(self):
        group_id = self._create_group(name="group-read-sync")

        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)
        alice.get_received()
        bob.get_received()

        self._emit_group_message(
            alice,
            {
                "group_id": group_id,
                "message": "enc-before-join-sync",
                "encrypted_keys": {
                    "alice": "alice-key-sync",
                    "bob": "bob-key-sync",
                },
            },
        )

        alice_events = alice.get_received()
        sent_events = [event for event in alice_events if event["name"] == "group_message_sent"]
        self.assertEqual(len(sent_events), 1)
        message_id = sent_events[0]["args"][0]["message_id"]

        # Bob may receive live group events while on the main screen, but unread
        # still remains until the group chat is explicitly opened and synced.
        bob.get_received()

        bob.emit("join_group", {"group_id": group_id})
        bob_after_join = bob.get_received()
        joined_events = [event for event in bob_after_join if event["name"] == "group_joined"]
        self.assertEqual(len(joined_events), 1)
        read_sync_events = [
            event for event in bob_after_join if event["name"] == "group_read_state_synced"
        ]
        self.assertEqual(len(read_sync_events), 1)
        self.assertEqual(read_sync_events[0]["args"][0]["group_id"], group_id)
        self.assertGreaterEqual(read_sync_events[0]["args"][0]["marked"], 1)

        alice_after_join = alice.get_received()
        delivered_status_events = [
            event
            for event in alice_after_join
            if event["name"] == "group_delivered_messages_status"
        ]
        seen_status_events = [
            event
            for event in alice_after_join
            if event["name"] == "group_seen_messages_status"
        ]
        self.assertTrue(
            any(
                event["args"][0].get("group_id") == group_id
                and message_id in event["args"][0].get("message_ids", [])
                for event in delivered_status_events
            )
        )
        self.assertTrue(
            any(
                event["args"][0].get("group_id") == group_id
                and message_id in event["args"][0].get("message_ids", [])
                for event in seen_status_events
            )
        )

        with self.app.app_context():
            pending_count = self.message_service.get_group_pending_count("bob", group_id)
            self.assertEqual(pending_count, 0)

    def test_group_send_requires_async_enqueue_in_production_when_worker_unavailable(self):
        group_id = self._create_group(name="prod-queue-required-group")

        old_env = self.app.config.get("APP_ENV")
        old_async_enabled = self.app.config.get("ASYNC_TASKS_ENABLED")
        old_inline_fallback = self.app.config.get("ASYNC_TASK_INLINE_FALLBACK")
        self.app.config["APP_ENV"] = "production"
        self.app.config["ASYNC_TASKS_ENABLED"] = True
        self.app.config["ASYNC_TASK_INLINE_FALLBACK"] = True

        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)
        alice.get_received()
        bob.get_received()

        try:
            with patch(
                "app.socket_events.async_task_service.enqueue_group_message_side_effects",
                return_value=False,
            ):
                with patch(
                    "app.socket_events.group_notification_service.dispatch_group_message_side_effects"
                ) as inline_mock:
                    self._emit_group_message(
                        alice,
                        {
                            "group_id": group_id,
                            "message": "enc-prod-queue-required",
                            "encrypted_keys": {
                                "alice": "alice-key",
                                "bob": "bob-key",
                            },
                            "client_message_id": "client-group-fail-1",
                        },
                    )
                    alice_events = alice.get_received()
                    bob_events = bob.get_received()

            inline_mock.assert_not_called()
        finally:
            self.app.config["APP_ENV"] = old_env
            self.app.config["ASYNC_TASKS_ENABLED"] = old_async_enabled
            self.app.config["ASYNC_TASK_INLINE_FALLBACK"] = old_inline_fallback

        sender_errors = [
            event for event in alice_events if event["name"] == "message_error"
        ]
        self.assertEqual(len(sender_errors), 1)
        self.assertIn(
            "temporarily unavailable",
            sender_errors[0]["args"][0].get("error", "").lower(),
        )
        self.assertEqual(
            sender_errors[0]["args"][0].get("code"),
            "group_side_effects_unavailable",
        )
        self.assertEqual(
            sender_errors[0]["args"][0].get("client_message_id"),
            "client-group-fail-1",
        )

        send_acks = [
            event for event in alice_events if event["name"] == "group_message_sent"
        ]
        self.assertEqual(len(send_acks), 0)

        bob_group_messages = [
            event for event in bob_events if event["name"] == "new_group_message"
        ]
        self.assertEqual(len(bob_group_messages), 0)

    def test_ack_marks_message_delivered_without_marking_seen(self):
        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)

        alice.get_received()
        bob.get_received()

        self._emit_private_message(
            alice,
            {"to": "bob", "message": "enc-delivered", "encrypted_key": "key-delivered"},
        )

        alice_events = alice.get_received()
        bob_events = bob.get_received()

        sent_events = [event for event in alice_events if event["name"] == "message_sent"]
        self.assertEqual(len(sent_events), 1)
        message_id = sent_events[0]["args"][0]["message_id"]

        incoming = [event for event in bob_events if event["name"] == "new_message"]
        self.assertEqual(len(incoming), 1)
        self.assertEqual(incoming[0]["args"][0]["message_id"], message_id)

        bob.emit("ack_messages", {"chat_id": "alice", "message_ids": [message_id]})
        bob.get_received()

        alice_after_ack = alice.get_received()
        delivered_events = [event for event in alice_after_ack if event["name"] == "message_delivered"]
        seen_events = [event for event in alice_after_ack if event["name"] == "message_seen"]

        self.assertEqual(len(delivered_events), 1)
        self.assertEqual(delivered_events[0]["args"][0]["message_id"], message_id)
        self.assertEqual(delivered_events[0]["args"][0]["chat_id"], "bob")
        self.assertEqual(len(seen_events), 0)

        alice.emit("get_delivered_messages", {"chat_id": "bob", "message_ids": [message_id]})
        delivered_status_events = [
            event for event in alice.get_received() if event["name"] == "delivered_messages_status"
        ]
        self.assertEqual(len(delivered_status_events), 1)
        self.assertIn(message_id, delivered_status_events[0]["args"][0]["message_ids"])

        alice.emit("get_seen_messages", {"chat_id": "bob", "message_ids": [message_id]})
        seen_status_events = [
            event for event in alice.get_received() if event["name"] == "seen_messages_status"
        ]
        self.assertEqual(len(seen_status_events), 1)
        self.assertEqual(seen_status_events[0]["args"][0]["message_ids"], [])

    def test_mark_read_messages_emits_seen_event(self):
        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)

        alice.get_received()
        bob.get_received()

        self._emit_private_message(
            alice,
            {"to": "bob", "message": "enc-read", "encrypted_key": "key-read"},
        )

        alice_events = alice.get_received()
        bob_events = bob.get_received()
        sent_events = [event for event in alice_events if event["name"] == "message_sent"]
        self.assertEqual(len(sent_events), 1)
        message_id = sent_events[0]["args"][0]["message_id"]

        incoming = [event for event in bob_events if event["name"] == "new_message"]
        self.assertEqual(len(incoming), 1)
        self.assertEqual(incoming[0]["args"][0]["message_id"], message_id)

        bob.emit("ack_messages", {"chat_id": "alice", "message_ids": [message_id]})
        bob.get_received()
        alice.get_received()

        bob.emit("mark_read_messages", {"chat_id": "alice", "message_ids": [message_id]})
        read_confirmed = [event for event in bob.get_received() if event["name"] == "read_confirmed"]
        self.assertEqual(len(read_confirmed), 1)
        self.assertEqual(read_confirmed[0]["args"][0]["marked"], 1)

        alice_after_read = alice.get_received()
        seen_events = [event for event in alice_after_read if event["name"] == "message_seen"]
        self.assertEqual(len(seen_events), 1)
        self.assertEqual(seen_events[0]["args"][0]["message_id"], message_id)
        self.assertEqual(seen_events[0]["args"][0]["chat_id"], "bob")

        alice.emit("get_seen_messages", {"chat_id": "bob", "message_ids": [message_id]})
        seen_status_events = [
            event for event in alice.get_received() if event["name"] == "seen_messages_status"
        ]
        self.assertEqual(len(seen_status_events), 1)
        self.assertIn(message_id, seen_status_events[0]["args"][0]["message_ids"])

    def test_sync_private_chat_read_state_clears_unread_and_emits_status_updates(self):
        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)

        alice.get_received()
        bob.get_received()

        self._emit_private_message(
            alice,
            {
                "to": "bob",
                "message": "enc-sync-open",
                "encrypted_key": "key-sync-open",
            },
        )

        alice_events = alice.get_received()
        bob_events = bob.get_received()
        sent_events = [event for event in alice_events if event["name"] == "message_sent"]
        self.assertEqual(len(sent_events), 1)
        message_id = sent_events[0]["args"][0]["message_id"]
        self.assertTrue(any(event["name"] == "new_message" for event in bob_events))

        bob.emit("sync_private_chat_read_state", {"chat_id": "alice"})
        bob_sync_events = bob.get_received()
        sync_events = [
            event for event in bob_sync_events if event["name"] == "private_chat_state_synced"
        ]
        self.assertEqual(len(sync_events), 1)
        self.assertEqual(sync_events[0]["args"][0]["chat_id"], "alice")
        self.assertGreaterEqual(sync_events[0]["args"][0]["marked_delivered"], 1)
        self.assertGreaterEqual(sync_events[0]["args"][0]["marked_seen"], 1)

        contacts_updates = [
            event for event in bob_sync_events if event["name"] == "contacts_updated"
        ]
        self.assertTrue(
            any(
                event["args"][0].get("contact") == "alice"
                and event["args"][0].get("has_unread") is False
                and int(event["args"][0].get("unread_count", 0)) == 0
                for event in contacts_updates
            )
        )

        alice_after_sync = alice.get_received()
        delivered_status_events = [
            event
            for event in alice_after_sync
            if event["name"] == "delivered_messages_status"
        ]
        seen_status_events = [
            event
            for event in alice_after_sync
            if event["name"] == "seen_messages_status"
        ]
        self.assertTrue(
            any(
                event["args"][0].get("chat_id") == "bob"
                and message_id in event["args"][0].get("message_ids", [])
                for event in delivered_status_events
            )
        )
        self.assertTrue(
            any(
                event["args"][0].get("chat_id") == "bob"
                and message_id in event["args"][0].get("message_ids", [])
                for event in seen_status_events
            )
        )

        with self.app.app_context():
            from app.services import notification_service

            unread_snapshot = notification_service.get_sender_unread_summary("bob", "alice")
            self.assertEqual(unread_snapshot.get("count", 0), 0)

    def test_ack_unknown_message_id_returns_removed_zero_with_reason(self):
        bob = self._connect(self.bob_token)
        bob.get_received()

        bob.emit(
            "ack_messages",
            {"chat_id": "alice", "message_ids": ["missing-message-id"]},
        )
        ack_events = [event for event in bob.get_received() if event["name"] == "ack_confirmed"]
        self.assertEqual(len(ack_events), 1)
        ack_payload = ack_events[0]["args"][0]
        self.assertEqual(ack_payload.get("removed"), 0)

        ignored = ack_payload.get("ignored", [])
        self.assertTrue(
            any(
                entry.get("reason") == "unknown_message_ids"
                and "missing-message-id" in entry.get("message_ids", [])
                for entry in ignored
            )
        )

    def test_ack_wrong_chat_scope_does_not_mark_message_delivered(self):
        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)
        carol = self._connect(self.carol_token)

        alice.get_received()
        bob.get_received()
        carol.get_received()

        self._emit_private_message(
            carol,
            {"to": "bob", "message": "enc-from-carol", "encrypted_key": "key-carol"},
        )

        carol_events = carol.get_received()
        sent_events = [event for event in carol_events if event["name"] == "message_sent"]
        self.assertEqual(len(sent_events), 1)
        message_id = sent_events[0]["args"][0]["message_id"]

        bob.get_received()
        alice.get_received()

        bob.emit("ack_messages", {"chat_id": "alice", "message_ids": [message_id]})
        bob_ack_events = [event for event in bob.get_received() if event["name"] == "ack_confirmed"]
        self.assertEqual(len(bob_ack_events), 1)
        ack_payload = bob_ack_events[0]["args"][0]
        self.assertEqual(ack_payload.get("removed"), 0)
        self.assertTrue(
            any(
                entry.get("reason") == "wrong_chat_scope"
                and message_id in entry.get("message_ids", [])
                for entry in ack_payload.get("ignored", [])
            )
        )

        self.assertEqual(
            [event for event in carol.get_received() if event["name"] == "message_delivered"],
            [],
        )

    def test_mark_read_wrong_chat_scope_does_not_emit_seen(self):
        bob = self._connect(self.bob_token)
        carol = self._connect(self.carol_token)

        bob.get_received()
        carol.get_received()

        self._emit_private_message(
            carol,
            {"to": "bob", "message": "enc-read-carol", "encrypted_key": "key-read-carol"},
        )
        carol_events = carol.get_received()
        sent_events = [event for event in carol_events if event["name"] == "message_sent"]
        self.assertEqual(len(sent_events), 1)
        message_id = sent_events[0]["args"][0]["message_id"]

        bob.get_received()
        carol.get_received()

        bob.emit(
            "mark_read_messages",
            {"chat_id": "alice", "message_ids": [message_id, "missing-for-read"]},
        )
        read_events = [event for event in bob.get_received() if event["name"] == "read_confirmed"]
        self.assertEqual(len(read_events), 1)
        read_payload = read_events[0]["args"][0]
        self.assertEqual(read_payload.get("marked"), 0)

        ignored = read_payload.get("ignored", [])
        self.assertTrue(
            any(
                entry.get("reason") == "wrong_chat_scope"
                and message_id in entry.get("message_ids", [])
                for entry in ignored
            )
        )
        self.assertTrue(
            any(
                entry.get("reason") == "unknown_message_ids"
                and "missing-for-read" in entry.get("message_ids", [])
                for entry in ignored
            )
        )

        self.assertEqual(
            [event for event in carol.get_received() if event["name"] == "message_seen"],
            [],
        )

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
        self._emit_private_message(
            alice,
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

    def test_post_share_message_type_is_delivered(self):
        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)

        alice.get_received()
        bob.get_received()

        self._emit_private_message(
            alice,
            {
                "to": "bob",
                "message": "[POST_SHARE]|42|alice|hello",
                "encrypted_key": "post-key-1",
                "type": "post",
            },
        )

        alice_events = alice.get_received()
        bob_events = bob.get_received()

        sent_events = [event for event in alice_events if event["name"] == "message_sent"]
        self.assertEqual(len(sent_events), 1)
        self.assertEqual(sent_events[0]["args"][0]["type"], "post")

        new_message_events = [event for event in bob_events if event["name"] == "new_message"]
        self.assertEqual(len(new_message_events), 1)
        payload = new_message_events[0]["args"][0]
        self.assertEqual(payload["type"], "post")
        self.assertEqual(payload["message"], "[POST_SHARE]|42|alice|hello")

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

    def test_user_status_transition_waits_for_last_active_connection(self):
        alice = self._connect(self.alice_token)
        alice.get_received()

        bob_first = self._connect(self.bob_token)
        first_online_events = [event for event in alice.get_received() if event["name"] == "user_status"]
        self.assertTrue(
            any(
                event["args"][0]["username"] == "bob" and event["args"][0]["online"]
                for event in first_online_events
            )
        )

        bob_second = self._connect(self.bob_token)
        second_online_events = [event for event in alice.get_received() if event["name"] == "user_status"]
        self.assertFalse(
            any(
                event["args"][0]["username"] == "bob" and event["args"][0]["online"]
                for event in second_online_events
            )
        )

        bob_first.disconnect()
        partial_disconnect_events = [event for event in alice.get_received() if event["name"] == "user_status"]
        self.assertFalse(
            any(
                event["args"][0]["username"] == "bob" and not event["args"][0]["online"]
                for event in partial_disconnect_events
            )
        )

        alice.emit("get_user_status", {"username": "bob"})
        queried_while_one_connection_left = [
            event for event in alice.get_received() if event["name"] == "user_status"
        ]
        self.assertTrue(
            any(
                event["args"][0]["username"] == "bob" and event["args"][0]["online"]
                for event in queried_while_one_connection_left
            )
        )

        bob_second.disconnect()
        final_disconnect_events = [event for event in alice.get_received() if event["name"] == "user_status"]
        self.assertTrue(
            any(
                event["args"][0]["username"] == "bob" and not event["args"][0]["online"]
                for event in final_disconnect_events
            )
        )

    def test_presence_heartbeat_restores_expired_connection_token(self):
        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)

        alice.get_received()
        bob.get_received()

        self.fake_redis.delete(self._presence_token_key_for_user("bob"))

        alice.emit("get_user_status", {"username": "bob"})
        offline_status_events = [
            event for event in alice.get_received() if event["name"] == "user_status"
        ]
        self.assertTrue(
            any(
                event["args"][0]["username"] == "bob" and not event["args"][0]["online"]
                for event in offline_status_events
            )
        )

        bob.emit("presence_heartbeat", {})
        heartbeat_ack_events = [
            event for event in bob.get_received() if event["name"] == "presence_heartbeat_ack"
        ]
        self.assertEqual(len(heartbeat_ack_events), 1)
        self.assertTrue(heartbeat_ack_events[0]["args"][0]["online"])

        recovered_online_events = [
            event for event in alice.get_received() if event["name"] == "user_status"
        ]
        self.assertTrue(
            any(
                event["args"][0]["username"] == "bob" and event["args"][0]["online"]
                for event in recovered_online_events
            )
        )

        alice.emit("get_user_status", {"username": "bob"})
        final_status_events = [
            event for event in alice.get_received() if event["name"] == "user_status"
        ]
        self.assertTrue(
            any(
                event["args"][0]["username"] == "bob" and event["args"][0]["online"]
                for event in final_status_events
            )
        )

    def test_stale_presence_token_cleanup_preserves_other_active_connection(self):
        alice = self._connect(self.alice_token)
        alice.get_received()

        bob_first = self._connect(self.bob_token)
        alice.get_received()
        bob_second = self._connect(self.bob_token)
        bob_second.get_received()

        if hasattr(self.socket_events, "_presence_state_lock"):
            with self.socket_events._presence_state_lock:
                first_sid = next(iter(set(self.socket_events._user_sids.get("bob", set()))))
        else:
            first_sid = next(iter(self.socket_events._user_sids.get("bob", set())))
        first_token = self.socket_events._presence_connection_token(first_sid)
        first_token_key = self.socket_events._presence_connection_token_key(first_token)
        self.fake_redis.delete(first_token_key)

        alice.emit("get_user_status", {"username": "bob"})
        status_events = [event for event in alice.get_received() if event["name"] == "user_status"]
        self.assertTrue(
            any(
                event["args"][0]["username"] == "bob" and event["args"][0]["online"]
                for event in status_events
            )
        )

    def test_group_online_users_snapshot_socket_event(self):
        group_id = self._create_group(name="group-online-snapshot")
        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)
        alice.get_received()
        bob.get_received()

        alice.emit("get_group_online_users", {"group_id": group_id})
        snapshot_events = [
            event for event in alice.get_received() if event["name"] == "group_online_users"
        ]
        self.assertEqual(len(snapshot_events), 1)

        payload = snapshot_events[0]["args"][0]
        self.assertEqual(payload.get("group_id"), group_id)
        usernames = {
            (user or {}).get("username")
            for user in payload.get("online_users", [])
        }
        self.assertIn("alice", usernames)
        self.assertIn("bob", usernames)

    def test_group_presence_changed_is_broadcast_to_group_members(self):
        group_id = self._create_group(name="group-presence-updates")
        alice = self._connect(self.alice_token)
        alice.get_received()

        bob = self._connect(self.bob_token)
        online_events = [
            event
            for event in alice.get_received()
            if event["name"] == "group_presence_changed"
        ]
        self.assertTrue(
            any(
                event["args"][0].get("group_id") == group_id
                and event["args"][0].get("online") is True
                and (event["args"][0].get("user") or {}).get("username") == "bob"
                for event in online_events
            )
        )

        bob.disconnect()
        offline_events = [
            event
            for event in alice.get_received()
            if event["name"] == "group_presence_changed"
        ]
        self.assertTrue(
            any(
                event["args"][0].get("group_id") == group_id
                and event["args"][0].get("online") is False
                and (event["args"][0].get("user") or {}).get("username") == "bob"
                for event in offline_events
            )
        )

    def test_group_online_users_http_endpoint_returns_online_members(self):
        group_id = self._create_group(name="group-online-http")
        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)
        alice.get_received()
        bob.get_received()

        http_client = self.app.test_client()
        response = http_client.get(
            f"/api/groups/{group_id}/online-users",
            headers=self._auth_headers(self.alice_token),
        )
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload.get("group_id"), group_id)
        usernames = {
            (user or {}).get("username")
            for user in payload.get("online_users", [])
        }
        self.assertIn("alice", usernames)
        self.assertIn("bob", usernames)

    def test_group_typing_event_is_broadcast_to_other_members(self):
        group_id = self._create_group(name="group-typing")
        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)
        alice.get_received()
        bob.get_received()

        alice.emit("group_typing", {"group_id": group_id, "is_typing": True})
        bob_events = [event for event in bob.get_received() if event["name"] == "group_typing"]
        self.assertEqual(len(bob_events), 1)
        payload = bob_events[0]["args"][0]
        self.assertEqual(payload.get("group_id"), group_id)
        self.assertTrue(payload.get("is_typing"))
        self.assertEqual((payload.get("user") or {}).get("username"), "alice")

        alice_events = [event for event in alice.get_received() if event["name"] == "group_typing"]
        self.assertEqual(len(alice_events), 0)

        alice.emit("group_typing", {"group_id": group_id, "is_typing": False})
        bob_stop_events = [event for event in bob.get_received() if event["name"] == "group_typing"]
        self.assertEqual(len(bob_stop_events), 1)
        self.assertFalse(bob_stop_events[0]["args"][0].get("is_typing"))

    def test_private_delete_for_me_is_persisted_in_history(self):
        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)
        alice.get_received()
        bob.get_received()

        self._emit_private_message(
            alice,
            {"to": "bob", "message": "enc-msg-delete-for-me", "encrypted_key": "key-delete-for-me"},
        )
        alice_events = alice.get_received()
        sent_events = [event for event in alice_events if event["name"] == "message_sent"]
        self.assertEqual(len(sent_events), 1)
        message_id = sent_events[0]["args"][0]["message_id"]
        bob.get_received()

        bob.emit(
            "delete_message_for_me",
            {"chat_id": "alice", "message_id": message_id},
        )
        bob_delete_events = bob.get_received()
        self.assertTrue(
            any(event["name"] == "message_deleted_for_me" for event in bob_delete_events)
        )

        http_client = self.app.test_client()
        bob_history_resp = http_client.get(
            "/api/messages/history/private/alice",
            headers=self._auth_headers(self.bob_token),
        )
        self.assertEqual(bob_history_resp.status_code, 200)
        bob_history_ids = {
            (message or {}).get("message_id")
            for message in bob_history_resp.get_json().get("messages", [])
        }
        self.assertNotIn(message_id, bob_history_ids)

        alice_history_resp = http_client.get(
            "/api/messages/history/private/bob",
            headers=self._auth_headers(self.alice_token),
        )
        self.assertEqual(alice_history_resp.status_code, 200)
        alice_history_ids = {
            (message or {}).get("message_id")
            for message in alice_history_resp.get_json().get("messages", [])
        }
        self.assertIn(message_id, alice_history_ids)

    def test_group_delete_for_me_is_persisted_in_history(self):
        group_id = self._create_group(name="group-delete-for-me")
        alice = self._connect(self.alice_token)
        bob = self._connect(self.bob_token)
        alice.get_received()
        bob.get_received()

        self._emit_group_message(
            alice,
            {
                "group_id": group_id,
                "message": "enc-group-delete-for-me",
                "encrypted_keys": {"alice": "alice-key", "bob": "bob-key"},
            },
        )
        alice_events = alice.get_received()
        sent_events = [event for event in alice_events if event["name"] == "group_message_sent"]
        self.assertEqual(len(sent_events), 1)
        message_id = sent_events[0]["args"][0]["message_id"]
        bob.get_received()

        bob.emit(
            "delete_group_message_for_me",
            {"group_id": group_id, "message_id": message_id},
        )
        bob_delete_events = bob.get_received()
        self.assertTrue(
            any(event["name"] == "group_message_deleted_for_me" for event in bob_delete_events)
        )

        http_client = self.app.test_client()
        bob_history_resp = http_client.get(
            f"/api/messages/history/group/{group_id}",
            headers=self._auth_headers(self.bob_token),
        )
        self.assertEqual(bob_history_resp.status_code, 200)
        bob_history_ids = {
            (message or {}).get("message_id")
            for message in bob_history_resp.get_json().get("messages", [])
        }
        self.assertNotIn(message_id, bob_history_ids)

        alice_history_resp = http_client.get(
            f"/api/messages/history/group/{group_id}",
            headers=self._auth_headers(self.alice_token),
        )
        self.assertEqual(alice_history_resp.status_code, 200)
        alice_history_ids = {
            (message or {}).get("message_id")
            for message in alice_history_resp.get_json().get("messages", [])
        }
        self.assertIn(message_id, alice_history_ids)

    def test_private_delete_for_everyone_rejects_unknown_message_id(self):
        alice = self._connect(self.alice_token)
        alice.get_received()

        alice.emit(
            "delete_message",
            {"chat_id": "bob", "message_id": "missing-message-id"},
        )
        error_events = [event for event in alice.get_received() if event["name"] == "message_error"]
        self.assertEqual(len(error_events), 1)
        self.assertIn("message not found", error_events[0]["args"][0].get("error", "").lower())

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

        self._emit_private_message(
            bob,
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
