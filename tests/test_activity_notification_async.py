import unittest
from unittest.mock import patch

from flask import Flask

from app.services import activity_notification_service


class TestActivityNotificationAsync(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config["ASYNC_TASKS_ENABLED"] = True
        self.app.config["ASYNC_TASK_INLINE_FALLBACK"] = True

    def test_notify_follow_uses_queue_when_enqueue_succeeds(self):
        with self.app.app_context():
            with patch(
                "app.services.activity_notification_service.async_task_service.enqueue_activity_notification_event",
                return_value=True,
            ) as enqueue_mock:
                with patch(
                    "app.services.activity_notification_service._notify_follow_sync"
                ) as inline_mock:
                    activity_notification_service.notify_follow("alice", "bob")

        enqueue_mock.assert_called_once()
        inline_mock.assert_not_called()

    def test_notify_follow_falls_back_inline_when_enqueue_fails(self):
        with self.app.app_context():
            with patch(
                "app.services.activity_notification_service.async_task_service.enqueue_activity_notification_event",
                return_value=False,
            ) as enqueue_mock:
                with patch(
                    "app.services.activity_notification_service._notify_follow_sync"
                ) as inline_mock:
                    activity_notification_service.notify_follow("alice", "bob")

        enqueue_mock.assert_called_once()
        inline_mock.assert_called_once_with("alice", "bob")


if __name__ == "__main__":
    unittest.main()
