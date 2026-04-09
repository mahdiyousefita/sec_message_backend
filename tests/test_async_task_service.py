import json
import unittest
from unittest.mock import patch

from flask import Flask

from app.services import async_task_service
from tests.fake_redis import FakeRedis


class _FailingRedis:
    def rpush(self, *_args, **_kwargs):
        raise RuntimeError("redis unavailable")


class TestAsyncTaskService(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config["ASYNC_TASKS_ENABLED"] = True
        self.app.config["ASYNC_TASK_QUEUE_NAME"] = "test:async:queue"
        self.app.config["ASYNC_TASK_WORKER_BLOCK_TIMEOUT_SECONDS"] = 1
        self.app.config["ASYNC_TASK_MAX_RETRIES"] = 1
        self.app.config["ASYNC_TASK_INLINE_FALLBACK"] = True
        self.fake_redis = FakeRedis()
        async_task_service._enqueue_client = None

    def test_enqueue_task_writes_task_envelope(self):
        with self.app.app_context():
            with patch.object(async_task_service, "redis_client", self.fake_redis):
                ok = async_task_service.enqueue_activity_notification_event(
                    {"event": "follow", "actor_username": "alice", "target_username": "bob"},
                    source="test",
                )
                self.assertTrue(ok)

        items = self.fake_redis.lrange("test:async:queue", 0, -1)
        self.assertEqual(len(items), 1)
        payload = json.loads(items[0])
        self.assertEqual(payload["task_type"], async_task_service.TASK_TYPE_ACTIVITY_NOTIFICATION)
        self.assertEqual(payload["payload"]["event"], "follow")
        self.assertEqual(payload["source"], "test")
        self.assertEqual(payload["attempt"], 0)

    def test_enqueue_task_returns_false_when_backend_fails(self):
        with self.app.app_context():
            with patch.object(async_task_service, "redis_client", _FailingRedis()):
                ok = async_task_service.enqueue_activity_notification_event(
                    {"event": "follow"},
                    source="test",
                )
                self.assertFalse(ok)

    def test_process_one_pending_task_dispatches_handler(self):
        envelope = {
            "task_id": "1",
            "task_type": async_task_service.TASK_TYPE_GROUP_MESSAGE_SIDE_EFFECTS,
            "payload": {"group_id": 1, "sender": "alice", "message_payload": {"message_id": "m1"}},
            "attempt": 0,
        }
        self.fake_redis.rpush("test:async:queue", json.dumps(envelope))

        with self.app.app_context():
            with patch.object(async_task_service, "redis_client", self.fake_redis):
                with patch.object(async_task_service, "_handle_group_message_side_effects") as handler:
                    processed = async_task_service.process_one_pending_task(block_timeout_seconds=0)

        self.assertTrue(processed)
        handler.assert_called_once_with(envelope["payload"])

    def test_failed_task_is_requeued_until_retry_limit(self):
        envelope = {
            "task_id": "1",
            "task_type": async_task_service.TASK_TYPE_MEDIA_POST_PROCESS,
            "payload": {"post_id": 1, "media_items": []},
            "attempt": 0,
        }
        self.fake_redis.rpush("test:async:queue", json.dumps(envelope))

        with self.app.app_context():
            with patch.object(async_task_service, "redis_client", self.fake_redis):
                with patch.object(
                    async_task_service,
                    "_handle_media_post_process",
                    side_effect=RuntimeError("boom"),
                ):
                    processed = async_task_service.process_one_pending_task(block_timeout_seconds=0)

        self.assertTrue(processed)
        queued = self.fake_redis.lrange("test:async:queue", 0, -1)
        self.assertEqual(len(queued), 1)
        retried = json.loads(queued[0])
        self.assertEqual(retried["attempt"], 1)
        self.assertEqual(retried["task_id"], "1")


if __name__ == "__main__":
    unittest.main()
