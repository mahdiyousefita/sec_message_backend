import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone


class TestAuthRegisterValidation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db_fd, cls.db_path = tempfile.mkstemp(suffix=".db")
        os.close(db_fd)
        os.environ["DATABASE_URL"] = f"sqlite:///{cls.db_path}"
        os.environ["JWT_SECRET_KEY"] = "test-secret"

        from app import create_app
        from app.db import db

        cls.app = create_app()
        cls.db = db
        cls.client = cls.app.test_client()

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.db_path):
            os.remove(cls.db_path)

    def setUp(self):
        with self.app.app_context():
            self.db.drop_all()
            self.db.create_all()

    def test_register_rejects_missing_password(self):
        response = self.client.post(
            "/api/auth/register",
            json={
                "username": "user_without_password",
                "public_key": "pub_key_value"
            }
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"], "Missing fields")

    def test_register_rejects_missing_public_key(self):
        response = self.client.post(
            "/api/auth/register",
            json={
                "username": "user_without_pubkey",
                "password": "pass123"
            }
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"], "Missing fields")

    def test_register_rejects_blank_public_key(self):
        response = self.client.post(
            "/api/auth/register",
            json={
                "username": "user_with_blank_pubkey",
                "password": "pass123",
                "public_key": "   "
            }
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"], "Missing fields")

    def test_pending_registration_is_expired_handles_mixed_timezone_awareness(self):
        from app.models.pending_registration_model import PendingRegistration

        pending = PendingRegistration(
            registration_id="reg-1",
            username="pending_user",
            password_hash="hash",
            public_key="pub",
            name="Pending User",
            expires_at=datetime.now(timezone.utc) - timedelta(seconds=5),
        )

        self.assertTrue(pending.is_expired(datetime.now()))

    def test_pending_registration_seconds_until_expiry_handles_mixed_timezone_awareness(self):
        from app.models.pending_registration_model import PendingRegistration

        pending = PendingRegistration(
            registration_id="reg-2",
            username="pending_user_2",
            password_hash="hash",
            public_key="pub",
            name="Pending User 2",
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=90),
        )

        remaining = pending.seconds_until_expiry(datetime.now())
        self.assertGreaterEqual(remaining, 1)


if __name__ == "__main__":
    unittest.main()

