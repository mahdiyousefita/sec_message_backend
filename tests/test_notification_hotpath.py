import unittest
from unittest.mock import patch

from tests.fake_redis import FakeRedis


class TestNotificationHotPath(unittest.TestCase):
    def setUp(self):
        from app.repositories import message_repository
        from app.services import notification_service

        self.message_repository = message_repository
        self.notification_service = notification_service
        self.fake_redis = FakeRedis()

        self.patches = [
            patch.object(message_repository, "redis_client", self.fake_redis),
            patch.object(notification_service, "redis_client", self.fake_redis),
        ]
        for patcher in self.patches:
            patcher.start()

    def tearDown(self):
        for patcher in self.patches:
            patcher.stop()

    def _push_message(self, sender, recipient, idx):
        payload = self.message_repository.build_message_payload(
            sender=sender,
            encrypted_message=f"enc-{idx}",
            encrypted_key=f"key-{idx}",
            message_type="text",
        )
        self.message_repository.push_message_payload(recipient, payload)
        return payload

    def test_unread_summary_uses_metadata_without_inbox_scan(self):
        self._push_message("alice", "bob", 1)

        def fail_lrange(*_args, **_kwargs):
            raise AssertionError("lrange should not be used in unread hot path")

        self.fake_redis.lrange = fail_lrange
        summary = self.notification_service.get_unread_summary_map("bob")
        self.assertEqual(summary["total"], 1)
        self.assertEqual(summary["per_sender"]["alice"]["count"], 1)

    def test_ack_updates_unread_metadata_without_full_rescan(self):
        m1 = self._push_message("alice", "bob", 1)
        m2 = self._push_message("alice", "bob", 2)
        self._push_message("carol", "bob", 3)

        removed = self.message_repository.ack_messages("bob", [m1["message_id"]])
        self.assertEqual(removed, 1)

        summary = self.notification_service.get_unread_summary_map("bob")
        self.assertEqual(summary["total"], 2)
        self.assertEqual(summary["per_sender"]["alice"]["count"], 1)
        self.assertEqual(summary["per_sender"]["carol"]["count"], 1)

        removed = self.message_repository.ack_messages("bob", [m2["message_id"]])
        self.assertEqual(removed, 1)

        summary = self.notification_service.get_unread_summary_map("bob")
        self.assertNotIn("alice", summary["per_sender"])
        self.assertEqual(summary["per_sender"]["carol"]["count"], 1)

    def test_ack_messages_with_payloads_returns_only_removed_ids(self):
        payload = self._push_message("alice", "bob", 1)
        removed, removed_payloads = self.message_repository.ack_messages_with_payloads(
            "bob",
            [payload["message_id"], "missing-id"],
        )
        self.assertEqual(removed, 1)
        self.assertEqual([item["message_id"] for item in removed_payloads], [payload["message_id"]])


if __name__ == "__main__":
    unittest.main()
