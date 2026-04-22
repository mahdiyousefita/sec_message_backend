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
        self.app.config["APP_ENV"] = "development"
        self.app.config["ASYNC_TASKS_ENABLED"] = True
        self.app.config["ASYNC_TASK_QUEUE_NAME"] = "test:async:queue"
        self.app.config["ASYNC_TASK_WORKER_BLOCK_TIMEOUT_SECONDS"] = 1
        self.app.config["ASYNC_TASK_MAX_RETRIES"] = 1
        self.app.config["ASYNC_TASK_RETRY_BACKOFF_BASE_SECONDS"] = 0.0
        self.app.config["ASYNC_TASK_RETRY_BACKOFF_MAX_SECONDS"] = 1.0
        self.app.config["ASYNC_TASK_INLINE_FALLBACK"] = True
        self.app.config["ASYNC_TASK_WORKER_STARTUP_STRICT"] = False
        self.app.config["ASYNC_TASK_WORKER_HEARTBEAT_STALE_SECONDS"] = 60
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
        queued = self.fake_redis.zrange("test:async:queue:retry", 0, -1)
        self.assertEqual(len(queued), 1)
        retried = json.loads(queued[0])
        self.assertEqual(retried["attempt"], 1)
        self.assertEqual(retried["task_id"], "1")

    def test_failed_task_is_recorded_in_dead_letter_queue_after_retry_budget(self):
        envelope = {
            "task_id": "dead-1",
            "task_type": async_task_service.TASK_TYPE_MEDIA_POST_PROCESS,
            "payload": {"post_id": 99, "media_items": []},
            "attempt": 1,
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
        failed_items = self.fake_redis.lrange("test:async:queue:failed", 0, -1)
        self.assertEqual(len(failed_items), 1)
        failed_task = json.loads(failed_items[0])
        self.assertEqual(failed_task["task_id"], "dead-1")
        self.assertIn("failure_error", failed_task)

    def test_retry_queue_promotes_due_task_back_to_primary_queue(self):
        envelope = {
            "task_id": "retry-1",
            "task_type": async_task_service.TASK_TYPE_MEDIA_POST_PROCESS,
            "payload": {"post_id": 12, "media_items": []},
            "attempt": 0,
        }
        self.fake_redis.rpush("test:async:queue", json.dumps(envelope))

        with self.app.app_context():
            with patch.object(async_task_service, "redis_client", self.fake_redis):
                with patch.object(
                    async_task_service,
                    "_handle_media_post_process",
                    side_effect=[RuntimeError("boom"), None],
                ):
                    first_processed = async_task_service.process_one_pending_task(block_timeout_seconds=0)
                    second_processed = async_task_service.process_one_pending_task(block_timeout_seconds=0)

        self.assertTrue(first_processed)
        self.assertTrue(second_processed)
        self.assertEqual(self.fake_redis.zcard("test:async:queue:retry"), 0)
        self.assertEqual(self.fake_redis.llen("test:async:queue"), 0)

    def test_group_side_effect_fallback_disabled_in_production(self):
        with self.app.app_context():
            self.app.config["APP_ENV"] = "production"
            self.app.config["ASYNC_TASK_INLINE_FALLBACK"] = True
            self.assertFalse(
                async_task_service.should_fallback_inline(
                    task_type=async_task_service.TASK_TYPE_GROUP_MESSAGE_SIDE_EFFECTS
                )
            )
            self.assertTrue(
                async_task_service.should_fallback_inline(
                    task_type=async_task_service.TASK_TYPE_ACTIVITY_NOTIFICATION
                )
            )

    def test_worker_capacity_check_respects_strict_mode(self):
        with self.app.app_context():
            self.app.config["APP_ENV"] = "production"
            self.app.config["ASYNC_TASK_WORKER_STARTUP_STRICT"] = True
            with patch.object(async_task_service, "redis_client", self.fake_redis):
                self.assertFalse(
                    async_task_service.verify_worker_capacity_for_startup(source="test")
                )
                async_task_service.record_worker_heartbeat(
                    worker_id="worker-test",
                    source="test_worker",
                )
                self.assertTrue(
                    async_task_service.verify_worker_capacity_for_startup(source="test")
                )


if __name__ == "__main__":
    unittest.main()
