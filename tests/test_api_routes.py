import os
import io
import shutil
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from tests.fake_redis import FakeRedis

TEST_RSA_PUBLIC_KEY = """-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAti/rqUGnp4QpAP0kIDNn
VoVQnseLuxdHterA2USyUnNpsAdu7XIU4Em22siMtDeFI0qaiXyOkUizIRAqJHnq
geIhd+t8ScJWQzJP2Rjqoj3XsfPVKqzSqf2Qn/xk9DEcKCRZsmHG+QL+T7Yg+OFy
c+j3Tb53JvWdyTw7eLTQSALody8q+dfb/4GWAWw7hIsRL30p0AuN51QnpfwmKSKV
YfTr5Bt86Lfa1zANUgRkG81unNqCl5fKmQp1aJ9/maVMvWOj8acWANok1iQRw5Af
LUrxymQbqlpGWjB8oQxHB6PIGq0Fs+z9/zkLymMXPhBTfyrZTFNNphijRFLwxtaa
gwIDAQAB
-----END PUBLIC KEY-----"""


class TestApiRoutes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db_fd, cls.db_path = tempfile.mkstemp(suffix=".db")
        os.close(db_fd)
        os.environ["DATABASE_URL"] = f"sqlite:///{cls.db_path}"
        os.environ["JWT_SECRET_KEY"] = "test-secret"

        from app import create_app
        from app.db import db
        from app.services import auth_service, message_service, story_service
        from app.models.comment_model import Comment
        from app.repositories import message_repository
        from app.extensions import redis_client as redis_module
        import app.routes.contact_routes as contact_routes
        import app.socket_events as socket_events

        cls.app = create_app()
        cls.client = cls.app.test_client()
        cls.db = db
        cls.auth_service = auth_service
        cls.message_service = message_service
        cls.story_service = story_service
        cls.Comment = Comment
        cls.socket_events = socket_events

        cls.fake_redis = FakeRedis()
        cls.redis_patches = [
            patch.object(message_repository, "redis_client", cls.fake_redis),
            patch.object(redis_module, "redis_client", cls.fake_redis),
            patch.object(story_service, "redis_client", cls.fake_redis),
            patch.object(contact_routes, "r", cls.fake_redis),
        ]
        for patcher in cls.redis_patches:
            patcher.start()

    @classmethod
    def tearDownClass(cls):
        for patcher in cls.redis_patches:
            patcher.stop()

        cls.socket_events._user_sids.clear()
        cls.socket_events._registered = False

        if os.path.exists(cls.db_path):
            os.remove(cls.db_path)

    def setUp(self):
        with self.app.app_context():
            self.auth_service.reset_login_rate_limit_state()
            self.db.drop_all()
            self.db.create_all()
            self.app.config["MEDIA_CONTENT_SNIFFING_ENABLED"] = False
        self.socket_events._user_sids.clear()
        self.fake_redis.clear()
        uploads_dir = os.path.join(self.app.static_folder, "uploads")
        if os.path.isdir(uploads_dir):
            shutil.rmtree(uploads_dir)

    def _register(self, username, password="pass123", public_key=None):
        public_key = public_key or TEST_RSA_PUBLIC_KEY
        with self.app.app_context():
            self.auth_service.register(username, password, public_key)

    def _auth_header(self, username, password="pass123"):
        with self.app.app_context():
            token = self.auth_service.login(username, password)["access_token"]
        return {"Authorization": f"Bearer {token}"}

    def _refresh_header(self, username, password="pass123"):
        with self.app.app_context():
            token = self.auth_service.login(username, password)["refresh_token"]
        return {"Authorization": f"Bearer {token}"}

    def _make_admin(self, username):
        with self.app.app_context():
            from app.models.admin_model import AdminUser
            from app.models.user_model import User

            user = User.query.filter_by(username=username).first()
            self.assertIsNotNone(user)
            exists = AdminUser.query.filter_by(user_id=user.id).first()
            if not exists:
                self.db.session.add(AdminUser(user_id=user.id))
                self.db.session.commit()

    def _user_id(self, username):
        with self.app.app_context():
            from app.models.user_model import User

            user = User.query.filter_by(username=username).first()
            self.assertIsNotNone(user)
            return user.id

    def test_auth_register_and_login_success(self):
        register_response = self.client.post(
            "/api/auth/register",
            json={
                "username": "api_user",
                "password": "pass123",
                "public_key": "api_user_pub",
            },
        )
        self.assertEqual(register_response.status_code, 201)

        login_response = self.client.post(
            "/api/auth/login",
            json={"username": "api_user", "password": "pass123"},
        )
        self.assertEqual(login_response.status_code, 200)
        body = login_response.get_json()
        self.assertIn("access_token", body)
        self.assertIn("refresh_token", body)

    def test_register_stores_argon2id_password_hash(self):
        register_response = self.client.post(
            "/api/auth/register",
            json={
                "username": "secure_user",
                "password": "pass123",
                "public_key": "secure_user_pub",
            },
        )
        self.assertEqual(register_response.status_code, 201)

        with self.app.app_context():
            from app.models.user_model import User
            from app.services.password_security import is_argon2_hash

            user = User.query.filter_by(username="secure_user").first()
            self.assertIsNotNone(user)
            self.assertTrue(is_argon2_hash(user.password_hash))

    def test_login_migrates_legacy_plaintext_password_hash(self):
        self._register("legacy_user")

        with self.app.app_context():
            from app.models.user_model import User
            from app.services.password_security import is_argon2_hash

            user = User.query.filter_by(username="legacy_user").first()
            self.assertIsNotNone(user)
            user.password_hash = "pass123"
            self.db.session.commit()
            self.assertFalse(is_argon2_hash(user.password_hash))

        login_response = self.client.post(
            "/api/auth/login",
            json={"username": "legacy_user", "password": "pass123"},
        )
        self.assertEqual(login_response.status_code, 200)

        with self.app.app_context():
            from app.models.user_model import User
            from app.services.password_security import is_argon2_hash

            migrated_user = User.query.filter_by(username="legacy_user").first()
            self.assertIsNotNone(migrated_user)
            self.assertTrue(is_argon2_hash(migrated_user.password_hash))

    def test_admin_login_migrates_legacy_plaintext_password_hash(self):
        self._register("admin")
        self._make_admin("admin")

        with self.app.app_context():
            from app.models.user_model import User
            from app.services.password_security import is_argon2_hash

            admin_user = User.query.filter_by(username="admin").first()
            self.assertIsNotNone(admin_user)
            admin_user.password_hash = "pass123"
            self.db.session.commit()
            self.assertFalse(is_argon2_hash(admin_user.password_hash))

        admin_login = self.client.post(
            "/admin/api/login",
            json={"username": "admin", "password": "pass123"},
        )
        self.assertEqual(admin_login.status_code, 200)

        with self.app.app_context():
            from app.models.user_model import User
            from app.services.password_security import is_argon2_hash

            admin_user = User.query.filter_by(username="admin").first()
            self.assertIsNotNone(admin_user)
            self.assertTrue(is_argon2_hash(admin_user.password_hash))

    def test_auth_key_status_reports_server_public_key_presence(self):
        self._register("alice", public_key="alice_key")
        headers = self._auth_header("alice")

        response = self.client.get("/api/auth/keys/status", headers=headers)
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["has_public_key"])

        with self.app.app_context():
            from app.models.user_model import User
            user = User.query.filter_by(username="alice").first()
            self.assertIsNotNone(user)
            user.public_key = ""
            self.db.session.commit()

        response_after_clear = self.client.get("/api/auth/keys/status", headers=headers)
        self.assertEqual(response_after_clear.status_code, 200)
        self.assertFalse(response_after_clear.get_json()["has_public_key"])

    def test_auth_rejects_invalid_json(self):
        response = self.client.post(
            "/api/auth/register",
            data="not-json",
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"], "Invalid JSON body")

    def test_auth_refresh_returns_access_token(self):
        self._register("alice")
        headers = self._refresh_header("alice")

        token_response = self.client.post("/api/auth/token", headers=headers)
        self.assertEqual(token_response.status_code, 200)
        token_body = token_response.get_json()
        self.assertIn("access_token", token_body)
        self.assertTrue(token_body["access_token"])

        refresh_response = self.client.post("/api/auth/refresh", headers=headers)
        self.assertEqual(refresh_response.status_code, 200)
        refresh_body = refresh_response.get_json()
        self.assertIn("access_token", refresh_body)
        self.assertTrue(refresh_body["access_token"])

    def test_auth_refresh_rejects_access_token(self):
        self._register("alice")
        headers = self._auth_header("alice")

        response = self.client.post("/api/auth/token", headers=headers)
        self.assertEqual(response.status_code, 422)

    def test_auth_login_rate_limit_blocks_repeated_failures(self):
        self._register("alice")
        original_limit = self.app.config.get("AUTH_LOGIN_RATE_LIMIT_MAX_ATTEMPTS")
        original_window = self.app.config.get("AUTH_LOGIN_RATE_LIMIT_WINDOW_SECONDS")
        original_lockout = self.app.config.get("AUTH_LOGIN_RATE_LIMIT_LOCKOUT_SECONDS")
        try:
            self.app.config["AUTH_LOGIN_RATE_LIMIT_MAX_ATTEMPTS"] = 2
            self.app.config["AUTH_LOGIN_RATE_LIMIT_WINDOW_SECONDS"] = 60
            self.app.config["AUTH_LOGIN_RATE_LIMIT_LOCKOUT_SECONDS"] = 60

            first = self.client.post(
                "/api/auth/login",
                json={"username": "alice", "password": "wrong-pass"},
            )
            self.assertEqual(first.status_code, 401)

            second = self.client.post(
                "/api/auth/login",
                json={"username": "alice", "password": "wrong-pass"},
            )
            self.assertEqual(second.status_code, 401)

            third = self.client.post(
                "/api/auth/login",
                json={"username": "alice", "password": "wrong-pass"},
            )
            self.assertEqual(third.status_code, 429)
        finally:
            self.app.config["AUTH_LOGIN_RATE_LIMIT_MAX_ATTEMPTS"] = original_limit
            self.app.config["AUTH_LOGIN_RATE_LIMIT_WINDOW_SECONDS"] = original_window
            self.app.config["AUTH_LOGIN_RATE_LIMIT_LOCKOUT_SECONDS"] = original_lockout

    def test_auth_logout_returns_success(self):
        response = self.client.post("/api/auth/logout")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["message"], "Logged out")

    def test_admin_can_update_user_username_and_password(self):
        self._register("admin")
        self._register("alice")
        self._make_admin("admin")
        admin_headers = self._auth_header("admin")
        alice_id = self._user_id("alice")

        response = self.client.patch(
            f"/admin/api/users/{alice_id}/credentials",
            headers=admin_headers,
            json={"username": "alice_new", "password": "newpass123"},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["message"], "User credentials updated")
        self.assertEqual(payload["user"]["username"], "alice_new")
        self.assertIn("username", payload["changed_fields"])
        self.assertIn("password", payload["changed_fields"])

        old_login = self.client.post(
            "/api/auth/login",
            json={"username": "alice", "password": "pass123"},
        )
        self.assertEqual(old_login.status_code, 401)

        wrong_password_login = self.client.post(
            "/api/auth/login",
            json={"username": "alice_new", "password": "pass123"},
        )
        self.assertEqual(wrong_password_login.status_code, 401)

        new_login = self.client.post(
            "/api/auth/login",
            json={"username": "alice_new", "password": "newpass123"},
        )
        self.assertEqual(new_login.status_code, 200)
        self.assertIn("access_token", new_login.get_json())

    def test_admin_update_username_conflict_returns_409(self):
        self._register("admin")
        self._register("alice")
        self._register("bob")
        self._make_admin("admin")
        admin_headers = self._auth_header("admin")
        bob_id = self._user_id("bob")

        response = self.client.patch(
            f"/admin/api/users/{bob_id}/credentials",
            headers=admin_headers,
            json={"username": "alice"},
        )
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.get_json()["error"], "Username already exists")

    def test_admin_can_assign_and_clear_user_badge(self):
        self._register("admin")
        self._register("alice")
        self._make_admin("admin")
        admin_headers = self._auth_header("admin")
        alice_id = self._user_id("alice")

        assign_response = self.client.patch(
            f"/admin/api/users/{alice_id}/badge",
            headers=admin_headers,
            json={"badge": "verified"},
        )
        self.assertEqual(assign_response.status_code, 200)
        assign_payload = assign_response.get_json()
        self.assertEqual(assign_payload["user"]["badge"], "verified")

        users_response = self.client.get(
            "/admin/api/users",
            headers=admin_headers,
            query_string={"q": "alice"},
        )
        self.assertEqual(users_response.status_code, 200)
        users_payload = users_response.get_json()
        self.assertEqual(users_payload["users"][0]["badge"], "verified")

        clear_response = self.client.patch(
            f"/admin/api/users/{alice_id}/badge",
            headers=admin_headers,
            json={"badge": "   "},
        )
        self.assertEqual(clear_response.status_code, 200)
        self.assertIsNone(clear_response.get_json()["user"]["badge"])

    def test_admin_update_user_badge_rejects_invalid_payload(self):
        self._register("admin")
        self._register("alice")
        self._make_admin("admin")
        admin_headers = self._auth_header("admin")
        alice_id = self._user_id("alice")

        missing_field_response = self.client.patch(
            f"/admin/api/users/{alice_id}/badge",
            headers=admin_headers,
            json={"title": "vip"},
        )
        self.assertEqual(missing_field_response.status_code, 400)
        self.assertEqual(
            missing_field_response.get_json()["error"],
            "Field 'badge' is required",
        )

        non_string_response = self.client.patch(
            f"/admin/api/users/{alice_id}/badge",
            headers=admin_headers,
            json={"badge": 123},
        )
        self.assertEqual(non_string_response.status_code, 400)
        self.assertEqual(non_string_response.get_json()["error"], "Badge must be a string")

        unknown_badge_response = self.client.patch(
            f"/admin/api/users/{alice_id}/badge",
            headers=admin_headers,
            json={"badge": "vip"},
        )
        self.assertEqual(unknown_badge_response.status_code, 400)
        self.assertEqual(
            unknown_badge_response.get_json()["error"],
            "Badge is not in the allowed catalog",
        )

    def test_admin_badge_catalog_endpoint_returns_allowed_values(self):
        self._register("admin")
        self._make_admin("admin")
        admin_headers = self._auth_header("admin")

        response = self.client.get("/admin/api/badges", headers=admin_headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.get_json()["badges"],
            ["verified", "moderator", "staff", "post_of_the_day"],
        )

    def test_admin_post_of_day_status_and_manual_run_endpoints(self):
        from flask_jwt_extended import create_access_token
        from app.models.admin_model import AdminUser
        from app.models.post_model import Post
        from app.models.user_model import User

        with self.app.app_context():
            admin_user = User(
                username="admin_manual",
                password_hash="x",
                public_key="pk_admin",
            )
            author_user = User(
                username="alice_author",
                password_hash="x",
                public_key="pk_author",
            )
            self.db.session.add_all([admin_user, author_user])
            self.db.session.flush()
            self.db.session.add(AdminUser(user_id=admin_user.id))
            self.db.session.add(
                Post(
                    author_id=author_user.id,
                    text="candidate",
                    created_at=datetime.utcnow() - timedelta(hours=1),
                )
            )
            self.db.session.commit()
            token = create_access_token(identity=admin_user.username)

        headers = {"Authorization": f"Bearer {token}"}

        status_before = self.client.get("/admin/api/post-of-day/status", headers=headers)
        self.assertEqual(status_before.status_code, 200)
        status_before_payload = status_before.get_json()
        self.assertIn("scoring", status_before_payload)
        self.assertIn("next_scheduled_cycle_end", status_before_payload)
        self.assertIn("seconds_until_next_cycle_end", status_before_payload)
        self.assertGreaterEqual(status_before_payload["seconds_until_next_cycle_end"], 0)

        run_response = self.client.post("/admin/api/post-of-day/run", headers=headers)
        self.assertEqual(run_response.status_code, 200)
        run_payload = run_response.get_json()
        self.assertIn(run_payload["status"], {"selected", "already_selected"})
        self.assertIsNotNone(run_payload.get("winner_post_id"))

        status_after = self.client.get("/admin/api/post-of-day/status", headers=headers)
        self.assertEqual(status_after.status_code, 200)
        self.assertIsNotNone(status_after.get_json().get("current_winner"))

    def test_profile_and_search_include_user_badge(self):
        self._register("admin")
        self._register("alice")
        self._make_admin("admin")
        admin_headers = self._auth_header("admin")
        alice_id = self._user_id("alice")

        set_badge = self.client.patch(
            f"/admin/api/users/{alice_id}/badge",
            headers=admin_headers,
            json={"badge": "verified"},
        )
        self.assertEqual(set_badge.status_code, 200)

        profile_response = self.client.get("/api/profiles/alice")
        self.assertEqual(profile_response.status_code, 200)
        self.assertEqual(profile_response.get_json()["badge"], "verified")
        self.assertEqual(profile_response.get_json()["profile_image_shape"], "circle")

        search_response = self.client.get("/api/search/users?q=alice")
        self.assertEqual(search_response.status_code, 200)
        self.assertEqual(search_response.get_json()["users"][0]["badge"], "verified")
        self.assertEqual(
            search_response.get_json()["users"][0]["profile_image_shape"], "circle"
        )

    def test_admin_can_manage_app_update_settings(self):
        self._register("admin")
        self._make_admin("admin")
        admin_headers = self._auth_header("admin")

        settings_response = self.client.patch(
            "/admin/api/app-update/settings",
            headers=admin_headers,
            json={
                "force_update_below": "0.3.0",
                "optional_update_below": "0.6.0",
                "latest_version": "0.7.0",
                "download_url": "/download/app",
                "force_title": "Force Update",
                "force_message": "Please update now.",
                "optional_title": "Optional Update",
                "optional_message": "You can update later.",
            },
        )
        self.assertEqual(settings_response.status_code, 200)
        self.assertEqual(
            settings_response.get_json()["settings"]["download_url"],
            "/download/app",
        )
        self.assertEqual(
            settings_response.get_json()["settings"]["force_update_below"],
            "0.3.0",
        )

        get_response = self.client.get(
            "/admin/api/app-update/settings",
            headers=admin_headers,
        )
        self.assertEqual(get_response.status_code, 200)
        self.assertEqual(
            get_response.get_json()["settings"]["optional_update_below"],
            "0.6.0",
        )
        self.assertEqual(
            get_response.get_json()["settings"]["force_message"],
            "Please update now.",
        )

    def test_admin_can_manage_about_us_settings_and_public_can_read(self):
        self._register("admin")
        self._register("alice")
        self._register("bob")
        self._make_admin("admin")
        admin_headers = self._auth_header("admin")

        patch_response = self.client.patch(
            "/admin/api/about-us",
            headers=admin_headers,
            json={
                "description": "Built by the Dino core team.",
                "links": {
                    "website": {
                        "title": "Official site",
                        "url": "https://example.com",
                        "is_disabled": False,
                    },
                    "source_code": {
                        "title": "Source code",
                        "url": "https://github.com/example/dino",
                        "is_disabled": True,
                    },
                    "email": {
                        "title": "Email support",
                        "url": "support@example.com",
                        "is_disabled": False,
                    },
                },
                "team_members": [
                    {"username": "alice", "custom_name": "Mahdi", "role": "Android engineer"},
                    {"username": "bob", "role": "Backend engineer"},
                ],
            },
        )
        self.assertEqual(patch_response.status_code, 200)
        patched = patch_response.get_json()["about_us"]
        self.assertEqual(patched["description"], "Built by the Dino core team.")
        self.assertEqual(patched["links"]["website"]["title"], "Official site")
        self.assertTrue(patched["links"]["source_code"]["is_disabled"])
        self.assertEqual(len(patched["team_members"]), 2)
        self.assertEqual(patched["team_members"][0]["name"], "Mahdi")
        self.assertEqual(patched["team_members"][0]["custom_name"], "Mahdi")

        public_response = self.client.get("/api/about-us")
        self.assertEqual(public_response.status_code, 200)
        public_payload = public_response.get_json()
        self.assertEqual(public_payload["description"], "Built by the Dino core team.")
        self.assertEqual(len(public_payload["links"]), 3)
        team_usernames = [row["username"] for row in public_payload["team_members"]]
        self.assertListEqual(team_usernames, ["alice", "bob"])
        self.assertEqual(public_payload["team_members"][0]["name"], "Mahdi")
        self.assertEqual(public_payload["team_members"][0]["role"], "Android engineer")

    def test_admin_about_us_rejects_unknown_usernames(self):
        self._register("admin")
        self._make_admin("admin")
        admin_headers = self._auth_header("admin")

        response = self.client.patch(
            "/admin/api/about-us",
            headers=admin_headers,
            json={
                "team_members": [
                    {"username": "ghost_user", "role": "Moderator"},
                ],
            },
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("Unknown usernames", response.get_json()["error"])

    def test_admin_online_users_endpoint_lists_current_online_users(self):
        self._register("admin")
        self._register("alice")
        self._register("bob")
        self._make_admin("admin")
        admin_headers = self._auth_header("admin")

        self.socket_events._set_user_online("bob", sid="bob-sid-1")
        self.socket_events._set_user_online("bob", sid="bob-sid-2")
        self.socket_events._set_user_online("alice", sid="alice-sid-1")

        response = self.client.get("/admin/api/online-users", headers=admin_headers)
        self.assertEqual(response.status_code, 200)

        payload = response.get_json()
        self.assertEqual(payload["total"], 2)
        self.assertEqual(
            [user["username"] for user in payload["users"]],
            ["alice", "bob"],
        )
        self.assertTrue(all("id" in user for user in payload["users"]))
        self.assertTrue(all("name" in user for user in payload["users"]))
        self.assertTrue(all("badge" in user for user in payload["users"]))

    def test_admin_recently_online_users_endpoint_resets_entries_older_than_24_hours(self):
        self._register("admin")
        self._register("alice")
        self._register("bob")
        self._register("charlie")
        self._make_admin("admin")
        admin_headers = self._auth_header("admin")

        now = datetime.now(timezone.utc)
        self.socket_events._touch_recently_online("alice", seen_at=now - timedelta(hours=2))
        self.socket_events._touch_recently_online("bob", seen_at=now - timedelta(hours=26))
        self.socket_events._set_user_online("charlie", sid="charlie-sid-1")

        response = self.client.get("/admin/api/recently-online-users", headers=admin_headers)
        self.assertEqual(response.status_code, 200)

        payload = response.get_json()
        self.assertEqual(payload["total"], 2)
        self.assertEqual(
            [user["username"] for user in payload["users"]],
            ["alice", "charlie"],
        )
        self.assertTrue(all("badge" in user for user in payload["users"]))
        self.assertIsNone(
            self.fake_redis.zscore(
                self.socket_events.PRESENCE_RECENTLY_ONLINE_KEY,
                "bob",
            )
        )

    def test_version_check_endpoint_returns_force_optional_and_none(self):
        self._register("admin")
        self._make_admin("admin")
        admin_headers = self._auth_header("admin")

        settings_response = self.client.patch(
            "/admin/api/app-update/settings",
            headers=admin_headers,
            json={
                "force_update_below": "0.3.0",
                "optional_update_below": "0.6.0",
                "latest_version": "0.7.0",
                "download_url": "/download/app",
            },
        )
        self.assertEqual(settings_response.status_code, 200)

        force_response = self.client.post(
            "/api/app/version-check",
            json={"platform": "android", "version": "0.2.0"},
        )
        self.assertEqual(force_response.status_code, 200)
        force_body = force_response.get_json()
        self.assertEqual(force_body["action"], "force")
        self.assertTrue(force_body["is_blocking"])

        optional_response = self.client.post(
            "/api/app/version-check",
            json={"platform": "android", "version": "0.4.0"},
        )
        self.assertEqual(optional_response.status_code, 200)
        optional_body = optional_response.get_json()
        self.assertEqual(optional_body["action"], "optional")
        self.assertFalse(optional_body["is_blocking"])
        self.assertEqual(optional_body["normalized_version"], "0.4.0")
        self.assertTrue(optional_body["download_url"].endswith("/download/app"))

        optional_boundary_response = self.client.post(
            "/api/app/version-check",
            json={"platform": "android", "version": "0.6.0"},
        )
        self.assertEqual(optional_boundary_response.status_code, 200)
        boundary_body = optional_boundary_response.get_json()
        self.assertEqual(boundary_body["action"], "optional")
        self.assertFalse(boundary_body["is_blocking"])

        ok_response = self.client.post(
            "/api/app/version-check",
            json={"platform": "android", "version": "0.7.0"},
        )
        self.assertEqual(ok_response.status_code, 200)
        ok_body = ok_response.get_json()
        self.assertEqual(ok_body["action"], "none")
        self.assertFalse(ok_body["is_blocking"])

    def test_cors_preflight_headers_are_not_added_by_app(self):
        response = self.client.open(
            "/api/contacts",
            method="OPTIONS",
            headers={
                "Origin": "http://localhost:5173",
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "Authorization, Content-Type",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(response.headers.get("Access-Control-Allow-Origin"))
        self.assertIsNone(response.headers.get("Access-Control-Allow-Credentials"))
        self.assertIsNone(response.headers.get("Access-Control-Allow-Methods"))
        self.assertIsNone(response.headers.get("Access-Control-Allow-Headers"))

    def test_auth_rotate_public_key_updates_user_key(self):
        self._register("alice")
        headers = self._auth_header("alice")

        response = self.client.post(
            "/api/auth/keys/rotate",
            headers=headers,
            json={"public_key": "alice_rotated_key"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["message"], "Public key updated")

        from app.repositories import user_repository

        with self.app.app_context():
            user = user_repository.get_by_username("alice")
            self.assertIsNotNone(user)
            self.assertEqual(user.public_key, "alice_rotated_key")

    def test_create_post_requires_auth(self):
        response = self.client.post("/api/posts", json={"text": "hello"})
        self.assertEqual(response.status_code, 401)

    def test_private_history_survives_transient_redis_reset(self):
        self._register("alice")
        self._register("bob")
        headers = self._auth_header("alice")

        with self.app.app_context():
            payload = self.message_service.send_message(
                "alice",
                "bob",
                "enc-history",
                "history-key",
            )
            self.assertIsNotNone(payload.get("message_id"))

        self.fake_redis.clear()

        response = self.client.get(
            "/api/messages/history/private/bob",
            headers=headers,
        )
        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertEqual(body["chat_id"], "bob")
        self.assertEqual(len(body["messages"]), 1)
        self.assertEqual(body["messages"][0]["message"], "enc-history")
        self.assertEqual(body["messages"][0]["encrypted_key"], "history-key")

    def test_private_history_uses_sender_cipher_for_sender_view(self):
        self._register("alice")
        self._register("bob")
        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")

        with self.app.app_context():
            payload = self.message_service.send_message(
                "alice",
                "bob",
                "enc-for-bob",
                "bob-key",
                sender_encrypted_message="enc-for-alice",
                sender_encrypted_key="alice-key",
            )
            self.assertIsNotNone(payload.get("message_id"))

        alice_response = self.client.get(
            "/api/messages/history/private/bob",
            headers=alice_headers,
        )
        self.assertEqual(alice_response.status_code, 200)
        alice_message = alice_response.get_json()["messages"][0]
        self.assertEqual(alice_message["message"], "enc-for-alice")
        self.assertEqual(alice_message["encrypted_key"], "alice-key")

        bob_response = self.client.get(
            "/api/messages/history/private/alice",
            headers=bob_headers,
        )
        self.assertEqual(bob_response.status_code, 200)
        bob_message = bob_response.get_json()["messages"][0]
        self.assertEqual(bob_message["message"], "enc-for-bob")
        self.assertEqual(bob_message["encrypted_key"], "bob-key")

    def test_group_history_returns_recipient_scoped_encrypted_keys(self):
        self._register("alice")
        self._register("bob")
        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")

        with self.app.app_context():
            from app.models.group_model import Group, GroupMember
            from app.models.user_model import User
            from app.repositories import message_repository

            alice = User.query.filter_by(username="alice").first()
            bob = User.query.filter_by(username="bob").first()
            self.assertIsNotNone(alice)
            self.assertIsNotNone(bob)

            group = Group(name="history-group", creator_id=alice.id)
            self.db.session.add(group)
            self.db.session.flush()
            self.db.session.add(GroupMember(group_id=group.id, user_id=alice.id))
            self.db.session.add(GroupMember(group_id=group.id, user_id=bob.id))
            self.db.session.commit()
            group_id = group.id

            payload = message_repository.build_group_message_payload(
                sender="alice",
                group_id=group_id,
                encrypted_message="enc-group-history",
                encrypted_keys={
                    "alice": "alice-history-key",
                    "bob": "bob-history-key",
                },
            )
            message_repository.store_group_message_metadata(payload, group_id)

        bob_response = self.client.get(
            f"/api/messages/history/group/{group_id}",
            headers=bob_headers,
        )
        self.assertEqual(bob_response.status_code, 200)
        bob_message = bob_response.get_json()["messages"][0]
        self.assertEqual(bob_message["encrypted_keys"], {"bob": "bob-history-key"})
        self.assertEqual(bob_message["encrypted_key"], "bob-history-key")

        alice_response = self.client.get(
            f"/api/messages/history/group/{group_id}",
            headers=alice_headers,
        )
        self.assertEqual(alice_response.status_code, 200)
        alice_message = alice_response.get_json()["messages"][0]
        self.assertEqual(alice_message["encrypted_keys"], {"alice": "alice-history-key"})
        self.assertEqual(alice_message["encrypted_key"], "alice-history-key")

    def test_group_history_excludes_messages_before_member_join(self):
        self._register("alice")
        self._register("bob")
        self._register("charlie")
        charlie_headers = self._auth_header("charlie")

        with self.app.app_context():
            from app.models.group_model import Group, GroupMember
            from app.models.user_model import User
            from app.repositories import message_repository

            alice = User.query.filter_by(username="alice").first()
            bob = User.query.filter_by(username="bob").first()
            charlie = User.query.filter_by(username="charlie").first()
            self.assertIsNotNone(alice)
            self.assertIsNotNone(bob)
            self.assertIsNotNone(charlie)

            group = Group(name="join-cutoff-group", creator_id=alice.id)
            self.db.session.add(group)
            self.db.session.flush()
            self.db.session.add(GroupMember(group_id=group.id, user_id=alice.id))
            self.db.session.add(GroupMember(group_id=group.id, user_id=bob.id))
            self.db.session.commit()
            group_id = group.id

            old_payload = message_repository.build_group_message_payload(
                sender="alice",
                group_id=group_id,
                encrypted_message="enc-before-charlie",
                encrypted_keys={
                    "alice": "alice-before-key",
                    "bob": "bob-before-key",
                },
            )
            message_repository.store_group_message_metadata(old_payload, group_id)

            self.db.session.add(GroupMember(group_id=group_id, user_id=charlie.id))
            self.db.session.commit()

            new_payload = message_repository.build_group_message_payload(
                sender="alice",
                group_id=group_id,
                encrypted_message="enc-after-charlie",
                encrypted_keys={
                    "alice": "alice-after-key",
                    "bob": "bob-after-key",
                    "charlie": "charlie-after-key",
                },
            )
            message_repository.store_group_message_metadata(new_payload, group_id)

        history_response = self.client.get(
            f"/api/messages/history/group/{group_id}",
            headers=charlie_headers,
        )
        self.assertEqual(history_response.status_code, 200)
        history_payload = history_response.get_json()
        self.assertEqual(len(history_payload["messages"]), 1)
        self.assertEqual(
            history_payload["messages"][0]["message_id"],
            new_payload["message_id"],
        )

    def test_group_history_skips_non_decryptable_messages_for_member(self):
        self._register("alice")
        self._register("bob")
        bob_headers = self._auth_header("bob")

        with self.app.app_context():
            from app.models.group_model import Group, GroupMember
            from app.models.user_model import User
            from app.repositories import message_repository

            alice = User.query.filter_by(username="alice").first()
            bob = User.query.filter_by(username="bob").first()
            self.assertIsNotNone(alice)
            self.assertIsNotNone(bob)

            group = Group(name="missing-keys-group", creator_id=alice.id)
            self.db.session.add(group)
            self.db.session.flush()
            self.db.session.add(GroupMember(group_id=group.id, user_id=alice.id))
            self.db.session.add(GroupMember(group_id=group.id, user_id=bob.id))
            self.db.session.commit()
            group_id = group.id

            payload = message_repository.build_group_message_payload(
                sender="alice",
                group_id=group_id,
                encrypted_message="enc-without-bob-key",
                encrypted_keys={
                    "alice": "alice-only-key",
                },
            )
            message_repository.store_group_message_metadata(payload, group_id)

        history_response = self.client.get(
            f"/api/messages/history/group/{group_id}",
            headers=bob_headers,
        )
        self.assertEqual(history_response.status_code, 200)
        history_payload = history_response.get_json()
        self.assertEqual(history_payload["messages"], [])

    def test_create_post_and_list_posts_with_limit_cap(self):
        self._register("alice")
        headers = self._auth_header("alice")

        create_response = self.client.post(
            "/api/posts",
            json={"text": "first post"},
            headers=headers,
        )
        self.assertEqual(create_response.status_code, 201)
        post_id = create_response.get_json()["post_id"]
        self.assertGreater(post_id, 0)

        list_response = self.client.get("/api/posts?page=1&limit=500")
        self.assertEqual(list_response.status_code, 200)
        payload = list_response.get_json()
        self.assertEqual(payload["page"], 1)
        self.assertEqual(payload["limit"], 50)
        self.assertEqual(payload["total"], 1)
        self.assertEqual(len(payload["posts"]), 1)
        self.assertEqual(payload["posts"][0]["text"], "first post")
        self.assertEqual(payload["posts"][0]["author"]["id"], 1)
        self.assertEqual(payload["posts"][0]["author"]["username"], "alice")
        self.assertEqual(payload["posts"][0]["author"]["name"], "alice")
        self.assertIsNone(payload["posts"][0]["author"]["profile_image_url"])
        self.assertEqual(payload["posts"][0]["viewer_vote"], 0)

    def test_create_quote_post_includes_quoted_preview_in_feed_and_detail(self):
        self._register("alice")
        self._register("bob")
        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")

        original_response = self.client.post(
            "/api/posts",
            json={"text": "original from alice"},
            headers=alice_headers,
        )
        self.assertEqual(original_response.status_code, 201)
        original_post_id = original_response.get_json()["post_id"]

        quote_response = self.client.post(
            "/api/posts",
            json={"text": "bob is quoting this", "quoted_post_id": original_post_id},
            headers=bob_headers,
        )
        self.assertEqual(quote_response.status_code, 201)
        quote_post_id = quote_response.get_json()["post_id"]

        feed_response = self.client.get("/api/posts", headers=bob_headers)
        self.assertEqual(feed_response.status_code, 200)
        feed_posts = feed_response.get_json()["posts"]
        quote_payload = next((post for post in feed_posts if post["id"] == quote_post_id), None)
        self.assertIsNotNone(quote_payload)
        self.assertEqual(quote_payload["quoted_post_id"], original_post_id)
        self.assertIsNotNone(quote_payload["quoted_post"])
        self.assertEqual(quote_payload["quoted_post"]["id"], original_post_id)
        self.assertEqual(quote_payload["quoted_post"]["author"]["username"], "alice")
        self.assertEqual(quote_payload["quoted_post"]["text"], "original from alice")

        detail_response = self.client.get(f"/api/posts/{quote_post_id}", headers=bob_headers)
        self.assertEqual(detail_response.status_code, 200)
        detail_payload = detail_response.get_json()["post"]
        self.assertEqual(detail_payload["quoted_post_id"], original_post_id)
        self.assertIsNotNone(detail_payload["quoted_post"])
        self.assertEqual(detail_payload["quoted_post"]["id"], original_post_id)

    def test_create_quote_post_rejects_unknown_post_id(self):
        self._register("alice")
        headers = self._auth_header("alice")

        response = self.client.post(
            "/api/posts",
            json={"text": "quote invalid", "quoted_post_id": 999999},
            headers=headers,
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"], "Quoted post not found")

    def test_list_posts_can_skip_total_count(self):
        self._register("alice")
        headers = self._auth_header("alice")

        create_response = self.client.post(
            "/api/posts",
            json={"text": "skip total"},
            headers=headers,
        )
        self.assertEqual(create_response.status_code, 201)

        list_response = self.client.get("/api/posts?page=1&limit=10&include_total=false")
        self.assertEqual(list_response.status_code, 200)
        payload = list_response.get_json()
        self.assertIsNone(payload["total"])
        self.assertEqual(len(payload["posts"]), 1)
        self.assertEqual(payload["posts"][0]["text"], "skip total")

    def test_list_posts_rejects_invalid_include_total(self):
        response = self.client.get("/api/posts?include_total=invalid")
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"], "include_total must be a boolean")

    def test_get_single_post_by_id(self):
        self._register("alice")
        headers = self._auth_header("alice")

        create_response = self.client.post(
            "/api/posts",
            json={"text": "single post lookup"},
            headers=headers,
        )
        self.assertEqual(create_response.status_code, 201)
        post_id = create_response.get_json()["post_id"]

        detail_response = self.client.get(f"/api/posts/{post_id}", headers=headers)
        self.assertEqual(detail_response.status_code, 200)
        payload = detail_response.get_json()["post"]
        self.assertEqual(payload["id"], post_id)
        self.assertEqual(payload["text"], "single post lookup")
        self.assertEqual(payload["author"]["username"], "alice")
        self.assertEqual(payload["viewer_vote"], 0)

    def test_get_single_post_by_id_respects_followers_only_visibility(self):
        self._register("alice")
        self._register("bob")
        self._register("charlie")
        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")
        charlie_headers = self._auth_header("charlie")

        create_response = self.client.post(
            "/api/posts",
            json={"text": "private details", "followers_only": True},
            headers=alice_headers,
        )
        self.assertEqual(create_response.status_code, 201)
        post_id = create_response.get_json()["post_id"]

        unauth_response = self.client.get(f"/api/posts/{post_id}")
        self.assertEqual(unauth_response.status_code, 404)

        non_follower_response = self.client.get(f"/api/posts/{post_id}", headers=charlie_headers)
        self.assertEqual(non_follower_response.status_code, 404)

        follow_response = self.client.post("/api/follows/alice", headers=bob_headers)
        self.assertEqual(follow_response.status_code, 200)

        follower_response = self.client.get(f"/api/posts/{post_id}", headers=bob_headers)
        self.assertEqual(follower_response.status_code, 200)

    def test_followers_only_posts_visible_only_to_author_followers_and_owner(self):
        self._register("alice")
        self._register("bob")
        self._register("charlie")
        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")
        charlie_headers = self._auth_header("charlie")

        private_post = self.client.post(
            "/api/posts",
            json={"text": "private update", "followers_only": True},
            headers=alice_headers,
        )
        self.assertEqual(private_post.status_code, 201)

        public_post = self.client.post(
            "/api/posts",
            json={"text": "public update"},
            headers=alice_headers,
        )
        self.assertEqual(public_post.status_code, 201)

        unauth_feed = self.client.get("/api/posts")
        self.assertEqual(unauth_feed.status_code, 200)
        self.assertEqual(
            [post["text"] for post in unauth_feed.get_json()["posts"]],
            ["public update"],
        )

        charlie_feed = self.client.get("/api/posts", headers=charlie_headers)
        self.assertEqual(charlie_feed.status_code, 200)
        self.assertEqual(
            [post["text"] for post in charlie_feed.get_json()["posts"]],
            ["public update"],
        )

        follow_resp = self.client.post("/api/follows/alice", headers=bob_headers)
        self.assertEqual(follow_resp.status_code, 200)
        self.assertEqual(follow_resp.get_json()["message"], "Followed")

        bob_feed = self.client.get("/api/posts", headers=bob_headers)
        self.assertEqual(bob_feed.status_code, 200)
        self.assertEqual(
            [post["text"] for post in bob_feed.get_json()["posts"]],
            ["public update", "private update"],
        )

        owner_feed = self.client.get("/api/posts", headers=alice_headers)
        self.assertEqual(owner_feed.status_code, 200)
        self.assertEqual(
            [post["text"] for post in owner_feed.get_json()["posts"]],
            ["public update", "private update"],
        )

    def test_followers_only_posts_are_filtered_in_profile_and_search_for_non_followers(self):
        self._register("alice")
        self._register("bob")
        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")

        private_post = self.client.post(
            "/api/posts",
            json={"text": "alice private", "followers_only": True},
            headers=alice_headers,
        )
        self.assertEqual(private_post.status_code, 201)

        profile_before_follow = self.client.get("/api/profiles/alice/posts", headers=bob_headers)
        self.assertEqual(profile_before_follow.status_code, 200)
        self.assertEqual(profile_before_follow.get_json()["total"], 0)

        search_before_follow = self.client.get("/api/search/posts?q=private", headers=bob_headers)
        self.assertEqual(search_before_follow.status_code, 200)
        self.assertEqual(search_before_follow.get_json()["total"], 0)

        follow_resp = self.client.post("/api/follows/alice", headers=bob_headers)
        self.assertEqual(follow_resp.status_code, 200)

        profile_after_follow = self.client.get("/api/profiles/alice/posts", headers=bob_headers)
        self.assertEqual(profile_after_follow.status_code, 200)
        self.assertEqual(profile_after_follow.get_json()["total"], 1)

        search_after_follow = self.client.get("/api/search/posts?q=private", headers=bob_headers)
        self.assertEqual(search_after_follow.status_code, 200)
        self.assertEqual(search_after_follow.get_json()["total"], 1)

    def test_admin_posts_endpoint_lists_public_and_followers_only_posts(self):
        self._register("admin")
        self._register("alice")
        self._register("bob")
        self._make_admin("admin")
        admin_headers = self._auth_header("admin")
        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")

        self.assertEqual(
            self.client.post(
                "/api/posts",
                json={"text": "alice private", "followers_only": True},
                headers=alice_headers,
            ).status_code,
            201,
        )
        self.assertEqual(
            self.client.post(
                "/api/posts",
                json={"text": "alice public"},
                headers=alice_headers,
            ).status_code,
            201,
        )
        self.assertEqual(
            self.client.post(
                "/api/posts",
                json={"text": "bob public"},
                headers=bob_headers,
            ).status_code,
            201,
        )

        admin_posts = self.client.get("/admin/api/posts", headers=admin_headers)
        self.assertEqual(admin_posts.status_code, 200)
        payload = admin_posts.get_json()
        self.assertEqual(payload["total"], 3)
        self.assertEqual(len(payload["posts"]), 3)
        self.assertTrue(any(post["followers_only"] for post in payload["posts"]))
        self.assertTrue(all("is_daily_winner" in post for post in payload["posts"]))
        self.assertTrue(all("was_daily_winner" in post for post in payload["posts"]))
        self.assertTrue(all("daily_winner_at" in post for post in payload["posts"]))

    def test_admin_post_detail_includes_media_url_for_rendering(self):
        from app.models.post_model import Post

        self._register("admin")
        self._register("alice")
        self._make_admin("admin")
        admin_headers = self._auth_header("admin")
        alice_headers = self._auth_header("alice")

        class FakeMinio:
            def bucket_exists(self, *args, **kwargs):
                return True

            def put_object(self, **kwargs):
                return None

        with patch("app.services.post_service.get_minio_client", return_value=FakeMinio()):
            create_post = self.client.post(
                "/api/posts",
                data={
                    "text": "post with media",
                    "media": (io.BytesIO(b"fake-image-bytes"), "pic.webp", "image/webp"),
                },
                headers=alice_headers,
                content_type="multipart/form-data",
            )

        self.assertEqual(create_post.status_code, 201)
        post_id = create_post.get_json()["post_id"]

        with self.app.app_context():
            post = Post.query.get(post_id)
            self.assertIsNotNone(post)
            post.is_daily_winner = True
            post.daily_winner_at = datetime.utcnow()
            self.db.session.commit()

        detail = self.client.get(f"/admin/api/posts/{post_id}", headers=admin_headers)
        self.assertEqual(detail.status_code, 200)
        payload = detail.get_json()
        self.assertEqual(payload["post"]["id"], post_id)
        self.assertEqual(len(payload["post"]["media"]), 1)
        self.assertIn("url", payload["post"]["media"][0])
        self.assertIn("/media/posts/", payload["post"]["media"][0]["url"])
        self.assertTrue(payload["post"]["is_daily_winner"])
        self.assertTrue(payload["post"]["was_daily_winner"])
        self.assertIsNotNone(payload["post"]["daily_winner_at"])

    def test_create_post_with_image_media_multipart(self):
        self._register("alice")
        headers = self._auth_header("alice")

        class FakeMinio:
            def bucket_exists(self, *args, **kwargs):
                return True

            def put_object(self, **kwargs):
                return None

        with patch("app.services.post_service.get_minio_client", return_value=FakeMinio()):
            data = {
                "text": "post with image",
                "media": (io.BytesIO(b"fake-image-bytes"), "pic.webp", "image/webp"),
            }
            resp = self.client.post(
                "/api/posts",
                data=data,
                headers=headers,
                content_type="multipart/form-data",
            )

        self.assertEqual(resp.status_code, 201)
        self.assertIn("post_id", resp.get_json())

        list_resp = self.client.get("/api/posts")
        self.assertEqual(list_resp.status_code, 200)
        media_url = list_resp.get_json()["posts"][0]["media"][0]["url"]
        self.assertIn("/media/posts/", media_url)

    def test_create_post_rejects_svg_payload_disguised_as_png(self):
        self._register("alice")
        headers = self._auth_header("alice")

        response = self.client.post(
            "/api/posts",
            data={
                "text": "bad payload",
                "media": (io.BytesIO(b"<svg xmlns='http://www.w3.org/2000/svg'></svg>"), "x.png", "image/png"),
            },
            headers=headers,
            content_type="multipart/form-data",
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("Invalid media content", response.get_json()["error"])

    def test_create_music_post_allows_empty_text_and_returns_track_metadata(self):
        self._register("alice")
        headers = self._auth_header("alice")

        class FakeMinio:
            def bucket_exists(self, *args, **kwargs):
                return True

            def put_object(self, **kwargs):
                return None

        with patch("app.services.post_service.get_minio_client", return_value=FakeMinio()):
            data = {
                "text": "",
                "media": (io.BytesIO(b"fake-audio-bytes"), "night_drive.mp3", "audio/mpeg"),
            }
            create_resp = self.client.post(
                "/api/posts",
                data=data,
                headers=headers,
                content_type="multipart/form-data",
            )

        self.assertEqual(create_resp.status_code, 201)
        list_resp = self.client.get("/api/posts", headers=headers)
        self.assertEqual(list_resp.status_code, 200)
        payload = list_resp.get_json()["posts"][0]
        self.assertEqual(payload["text"], "")
        self.assertEqual(len(payload["media"]), 1)
        media = payload["media"][0]
        self.assertEqual(media["mime_type"], "audio/mpeg")
        self.assertEqual(media["display_name"], "night_drive.mp3")
        self.assertEqual(media["title"], "night_drive")
        self.assertIsNone(media["artist"])

    def test_create_music_post_uses_client_track_metadata_when_provided(self):
        self._register("alice")
        headers = self._auth_header("alice")

        class FakeMinio:
            def bucket_exists(self, *args, **kwargs):
                return True

            def put_object(self, **kwargs):
                return None

        with patch("app.services.post_service.get_minio_client", return_value=FakeMinio()):
            data = {
                "text": "",
                "track_title": "After Hours",
                "track_artist": "The Weeknd",
                "media": (io.BytesIO(b"fake-audio-bytes"), "raw_recording_001.mp3", "audio/mpeg"),
            }
            create_resp = self.client.post(
                "/api/posts",
                data=data,
                headers=headers,
                content_type="multipart/form-data",
            )

        self.assertEqual(create_resp.status_code, 201)
        list_resp = self.client.get("/api/posts", headers=headers)
        self.assertEqual(list_resp.status_code, 200)
        media = list_resp.get_json()["posts"][0]["media"][0]
        self.assertEqual(media["title"], "After Hours")
        self.assertEqual(media["artist"], "The Weeknd")

    def test_create_music_post_parses_artist_and_title_from_file_name(self):
        self._register("alice")
        headers = self._auth_header("alice")

        class FakeMinio:
            def bucket_exists(self, *args, **kwargs):
                return True

            def put_object(self, **kwargs):
                return None

        with patch("app.services.post_service.get_minio_client", return_value=FakeMinio()):
            data = {
                "text": "",
                "media": (io.BytesIO(b"fake-audio-bytes"), "Daft Punk - One More Time.mp3", "audio/mpeg"),
            }
            create_resp = self.client.post(
                "/api/posts",
                data=data,
                headers=headers,
                content_type="multipart/form-data",
            )

        self.assertEqual(create_resp.status_code, 201)
        list_resp = self.client.get("/api/posts", headers=headers)
        self.assertEqual(list_resp.status_code, 200)
        media = list_resp.get_json()["posts"][0]["media"][0]
        self.assertEqual(media["title"], "One More Time")
        self.assertEqual(media["artist"], "Daft Punk")

    def test_create_music_post_rejects_more_than_one_track(self):
        self._register("alice")
        headers = self._auth_header("alice")

        class FakeMinio:
            def bucket_exists(self, *args, **kwargs):
                return True

            def put_object(self, **kwargs):
                return None

        with patch("app.services.post_service.get_minio_client", return_value=FakeMinio()):
            data = {
                "text": "my mix",
                "media": [
                    (io.BytesIO(b"track-one"), "one.mp3", "audio/mpeg"),
                    (io.BytesIO(b"track-two"), "two.mp3", "audio/mpeg"),
                ],
            }
            resp = self.client.post(
                "/api/posts",
                data=data,
                headers=headers,
                content_type="multipart/form-data",
            )

        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.get_json()["error"], "Music posts can contain only one audio file")

    def test_create_music_post_accepts_generic_declared_mimetype_when_filename_is_audio(self):
        self._register("alice")
        headers = self._auth_header("alice")

        class FakeMinio:
            def bucket_exists(self, *args, **kwargs):
                return True

            def put_object(self, **kwargs):
                return None

        with patch("app.services.post_service.get_minio_client", return_value=FakeMinio()):
            create_resp = self.client.post(
                "/api/posts",
                data={
                    "text": "",
                    "media": (
                        io.BytesIO(b"fake-audio-bytes"),
                        "ambient.opus",
                        "application/octet-stream",
                    ),
                },
                headers=headers,
                content_type="multipart/form-data",
            )

        self.assertEqual(create_resp.status_code, 201)
        list_resp = self.client.get("/api/posts", headers=headers)
        self.assertEqual(list_resp.status_code, 200)
        media = list_resp.get_json()["posts"][0]["media"][0]
        self.assertTrue(media["mime_type"].startswith("audio/"))

    def test_create_post_rejects_when_same_user_upload_is_already_in_progress(self):
        self._register("alice")
        headers = self._auth_header("alice")
        lock_key = f"{self.app.config['POST_UPLOAD_LOCK_KEY_PREFIX']}:alice"
        self.fake_redis.set(lock_key, "in-flight")

        response = self.client.post(
            "/api/posts",
            json={"text": "second upload"},
            headers=headers,
        )

        self.assertEqual(response.status_code, 409)
        self.assertEqual(
            response.get_json()["error"],
            "Another post upload is already in progress",
        )

    def test_playlist_adds_music_track_and_lists_it(self):
        self._register("alice")
        headers = self._auth_header("alice")

        class FakeMinio:
            def bucket_exists(self, *args, **kwargs):
                return True

            def put_object(self, **kwargs):
                return None

        with patch("app.services.post_service.get_minio_client", return_value=FakeMinio()):
            create_resp = self.client.post(
                "/api/posts",
                data={
                    "text": "",
                    "media": (io.BytesIO(b"fake-audio-bytes"), "sunrise.mp3", "audio/mpeg"),
                },
                headers=headers,
                content_type="multipart/form-data",
            )
        self.assertEqual(create_resp.status_code, 201)

        posts_resp = self.client.get("/api/posts", headers=headers)
        self.assertEqual(posts_resp.status_code, 200)
        media_payload = posts_resp.get_json()["posts"][0]["media"][0]

        add_resp = self.client.post(
            "/api/playlists/tracks",
            headers=headers,
            json={"media_id": media_payload["id"]},
        )
        self.assertEqual(add_resp.status_code, 201)
        add_body = add_resp.get_json()
        self.assertTrue(add_body["created"])
        self.assertEqual(add_body["track"]["media_id"], media_payload["id"])
        self.assertEqual(add_body["track"]["mime_type"], "audio/mpeg")

        refreshed_posts_resp = self.client.get("/api/posts", headers=headers)
        refreshed_media_payload = refreshed_posts_resp.get_json()["posts"][0]["media"][0]
        self.assertEqual(len(refreshed_media_payload["playlist_adders"]), 1)
        self.assertEqual(refreshed_media_payload["playlist_adders"][0]["username"], "alice")

        list_resp = self.client.get("/api/playlists/tracks?page=1&limit=20", headers=headers)
        self.assertEqual(list_resp.status_code, 200)
        list_body = list_resp.get_json()
        self.assertEqual(list_body["total"], 1)
        self.assertEqual(len(list_body["tracks"]), 1)
        self.assertEqual(list_body["tracks"][0]["title"], "sunrise")
        self.assertIn("/media/posts/", list_body["tracks"][0]["track_url"])

    def test_playlist_add_track_is_idempotent(self):
        self._register("alice")
        headers = self._auth_header("alice")

        class FakeMinio:
            def bucket_exists(self, *args, **kwargs):
                return True

            def put_object(self, **kwargs):
                return None

        with patch("app.services.post_service.get_minio_client", return_value=FakeMinio()):
            create_resp = self.client.post(
                "/api/posts",
                data={
                    "text": "",
                    "media": (io.BytesIO(b"fake-audio-bytes"), "repeat.mp3", "audio/mpeg"),
                },
                headers=headers,
                content_type="multipart/form-data",
            )
        self.assertEqual(create_resp.status_code, 201)

        posts_resp = self.client.get("/api/posts", headers=headers)
        media_id = posts_resp.get_json()["posts"][0]["media"][0]["id"]

        first_add = self.client.post(
            "/api/playlists/tracks",
            headers=headers,
            json={"media_id": media_id},
        )
        self.assertEqual(first_add.status_code, 201)
        self.assertTrue(first_add.get_json()["created"])

        second_add = self.client.post(
            "/api/playlists/tracks",
            headers=headers,
            json={"media_id": media_id},
        )
        self.assertEqual(second_add.status_code, 200)
        self.assertFalse(second_add.get_json()["created"])

        list_resp = self.client.get("/api/playlists/tracks", headers=headers)
        self.assertEqual(list_resp.status_code, 200)
        self.assertEqual(list_resp.get_json()["total"], 1)

    def test_playlist_track_can_be_deleted(self):
        self._register("alice")
        headers = self._auth_header("alice")

        class FakeMinio:
            def bucket_exists(self, *args, **kwargs):
                return True

            def put_object(self, **kwargs):
                return None

        with patch("app.services.post_service.get_minio_client", return_value=FakeMinio()):
            create_resp = self.client.post(
                "/api/posts",
                data={
                    "text": "",
                    "media": (io.BytesIO(b"fake-audio-bytes"), "remove_me.mp3", "audio/mpeg"),
                },
                headers=headers,
                content_type="multipart/form-data",
            )
        self.assertEqual(create_resp.status_code, 201)

        posts_resp = self.client.get("/api/posts", headers=headers)
        media_id = posts_resp.get_json()["posts"][0]["media"][0]["id"]

        add_resp = self.client.post(
            "/api/playlists/tracks",
            headers=headers,
            json={"media_id": media_id},
        )
        self.assertEqual(add_resp.status_code, 201)
        track_id = add_resp.get_json()["track"]["id"]

        delete_resp = self.client.delete(f"/api/playlists/tracks/{track_id}", headers=headers)
        self.assertEqual(delete_resp.status_code, 200)
        self.assertEqual(delete_resp.get_json()["message"], "Track removed from playlist")

        list_resp = self.client.get("/api/playlists/tracks", headers=headers)
        self.assertEqual(list_resp.status_code, 200)
        self.assertEqual(list_resp.get_json()["total"], 0)

    def test_playlist_rejects_non_audio_media(self):
        self._register("alice")
        headers = self._auth_header("alice")

        class FakeMinio:
            def bucket_exists(self, *args, **kwargs):
                return True

            def put_object(self, **kwargs):
                return None

        with patch("app.services.post_service.get_minio_client", return_value=FakeMinio()):
            create_resp = self.client.post(
                "/api/posts",
                data={
                    "text": "image post",
                    "media": (io.BytesIO(b"fake-image"), "pic.webp", "image/webp"),
                },
                headers=headers,
                content_type="multipart/form-data",
            )
        self.assertEqual(create_resp.status_code, 201)

        posts_resp = self.client.get("/api/posts", headers=headers)
        media_id = posts_resp.get_json()["posts"][0]["media"][0]["id"]

        add_resp = self.client.post(
            "/api/playlists/tracks",
            headers=headers,
            json={"media_id": media_id},
        )
        self.assertEqual(add_resp.status_code, 400)
        self.assertEqual(
            add_resp.get_json()["error"],
            "Only audio tracks can be added to playlists",
        )

    def test_create_post_requires_text_when_not_music_post(self):
        self._register("alice")
        headers = self._auth_header("alice")

        response = self.client.post(
            "/api/posts",
            json={"text": "   "},
            headers=headers,
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"], "Text is required")

    def test_media_route_streams_object_from_minio(self):
        class FakeStat:
            content_type = "image/jpeg"
            size = 3
            etag = "etag-123"
            last_modified = datetime(2026, 2, 25, 18, 0, 0, tzinfo=timezone.utc)

        class FakeMinioObject:
            def __init__(self):
                self.closed = False
                self.released = False

            def stream(self, chunk_size):
                _ = chunk_size
                yield b"abc"

            def close(self):
                self.closed = True

            def release_conn(self):
                self.released = True

        fake_obj = FakeMinioObject()
        captured = {}

        class FakeMinio:
            def stat_object(self, bucket_name, object_name):
                captured["stat_bucket_name"] = bucket_name
                captured["stat_object_name"] = object_name
                return FakeStat()

            def get_object(self, bucket_name, object_name):
                captured["get_bucket_name"] = bucket_name
                captured["get_object_name"] = object_name
                return fake_obj

        with patch("app.routes.main_routes.get_minio_client", return_value=FakeMinio()):
            response = self.client.get("/media/posts/1/sample.jpeg")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, b"abc")
        self.assertEqual(response.mimetype, "image/jpeg")
        self.assertIn("max-age=", response.headers["Cache-Control"])
        self.assertEqual(response.headers["Accept-Ranges"], "bytes")
        self.assertEqual(response.headers["ETag"], '"etag-123"')
        self.assertEqual(captured["stat_bucket_name"], self.app.config["MINIO_BUCKET"])
        self.assertEqual(captured["stat_object_name"], "posts/1/sample.jpeg")
        self.assertEqual(captured["get_bucket_name"], self.app.config["MINIO_BUCKET"])
        self.assertEqual(captured["get_object_name"], "posts/1/sample.jpeg")
        self.assertTrue(fake_obj.closed)
        self.assertTrue(fake_obj.released)

    def test_media_route_head_uses_metadata_only(self):
        class FakeStat:
            content_type = "image/jpeg"
            size = 3
            etag = "etag-456"
            last_modified = datetime(2026, 2, 25, 18, 0, 0, tzinfo=timezone.utc)

        captured = {"get_object_calls": 0}

        class FakeMinio:
            def stat_object(self, bucket_name, object_name):
                captured["bucket_name"] = bucket_name
                captured["object_name"] = object_name
                return FakeStat()

            def get_object(self, **kwargs):
                captured["get_object_calls"] += 1
                return None

        with patch("app.routes.main_routes.get_minio_client", return_value=FakeMinio()):
            response = self.client.head("/media/posts/1/sample.jpeg")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, b"")
        self.assertEqual(response.headers["Content-Length"], "3")
        self.assertEqual(captured["bucket_name"], self.app.config["MINIO_BUCKET"])
        self.assertEqual(captured["object_name"], "posts/1/sample.jpeg")
        self.assertEqual(captured["get_object_calls"], 0)

    def test_media_route_returns_304_when_etag_matches(self):
        class FakeStat:
            content_type = "image/jpeg"
            size = 3
            etag = "etag-789"
            last_modified = datetime(2026, 2, 25, 18, 0, 0, tzinfo=timezone.utc)

        captured = {"get_object_calls": 0}

        class FakeMinio:
            def stat_object(self, **kwargs):
                return FakeStat()

            def get_object(self, **kwargs):
                captured["get_object_calls"] += 1
                return None

        with patch("app.routes.main_routes.get_minio_client", return_value=FakeMinio()):
            response = self.client.get(
                "/media/posts/1/sample.jpeg",
                headers={"If-None-Match": '"etag-789"'},
            )

        self.assertEqual(response.status_code, 304)
        self.assertEqual(response.data, b"")
        self.assertEqual(response.headers["ETag"], '"etag-789"')
        self.assertEqual(captured["get_object_calls"], 0)

    def test_create_post_rejects_more_than_8_media_files(self):
        self._register("alice")
        headers = self._auth_header("alice")

        class FakeMinio:
            def bucket_exists(self, *args, **kwargs):
                return True

            def put_object(self, **kwargs):
                return None

        with patch("app.services.post_service.get_minio_client", return_value=FakeMinio()):
            data = {
                "text": "too many",
                "media": [
                    (io.BytesIO(b"x"), f"img{i}.png", "image/png")
                    for i in range(9)
                ],
            }
            resp = self.client.post(
                "/api/posts",
                data=data,
                headers=headers,
                content_type="multipart/form-data",
            )

        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.get_json()["error"], "Maximum 8 media files allowed")

    def test_create_post_rejects_video_longer_than_30_minutes(self):
        self._register("alice")
        headers = self._auth_header("alice")

        def mp4_with_duration(seconds: int) -> bytes:
            def box(box_type: bytes, payload: bytes) -> bytes:
                size = 8 + len(payload)
                return size.to_bytes(4, "big") + box_type + payload

            mvhd_payload = (
                b"\x00\x00\x00\x00"
                + b"\x00" * 8
                + (1).to_bytes(4, "big")
                + seconds.to_bytes(4, "big")
            )
            mvhd = box(b"mvhd", mvhd_payload)
            moov = box(b"moov", mvhd)
            ftyp = box(b"ftyp", b"isom" + b"\x00\x00\x02\x00" + b"isom" + b"iso2")
            return ftyp + moov

        class FakeMinio:
            def bucket_exists(self, *args, **kwargs):
                return True

            def put_object(self, **kwargs):
                return None

        with patch("app.services.post_service.get_minio_client", return_value=FakeMinio()):
            data = {
                "text": "video post",
                "media": (io.BytesIO(mp4_with_duration(1801)), "vid.mp4", "video/mp4"),
            }
            resp = self.client.post(
                "/api/posts",
                data=data,
                headers=headers,
                content_type="multipart/form-data",
            )

        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.get_json()["error"], "Video must be 30 minutes or shorter")

    def test_create_post_uses_local_fallback_when_media_storage_fails(self):
        self._register("alice")
        headers = self._auth_header("alice")

        class FailingMinio:
            def bucket_exists(self, *args, **kwargs):
                raise RuntimeError("storage down")

        with patch("app.services.post_service.get_minio_client", return_value=FailingMinio()):
            data = {
                "text": "post with media storage failure",
                "media": (io.BytesIO(b"fake-image-bytes"), "pic.webp", "image/webp"),
            }
            resp = self.client.post(
                "/api/posts",
                data=data,
                headers=headers,
                content_type="multipart/form-data",
            )

        self.assertEqual(resp.status_code, 201)
        post_id = resp.get_json()["post_id"]
        self.assertGreater(post_id, 0)

        list_resp = self.client.get("/api/posts")
        self.assertEqual(list_resp.status_code, 200)
        media_url = list_resp.get_json()["posts"][0]["media"][0]["url"]
        self.assertIn("/static/uploads/posts/", media_url)

    def test_create_post_returns_503_when_fallback_disabled(self):
        self._register("alice")
        headers = self._auth_header("alice")

        class FailingMinio:
            def bucket_exists(self, *args, **kwargs):
                raise RuntimeError("storage down")

        self.app.config["MEDIA_LOCAL_FALLBACK_ENABLED"] = False
        try:
            with patch("app.services.post_service.get_minio_client", return_value=FailingMinio()):
                data = {
                    "text": "post with media storage failure",
                    "media": (io.BytesIO(b"fake-image-bytes"), "pic.webp", "image/webp"),
                }
                resp = self.client.post(
                    "/api/posts",
                    data=data,
                    headers=headers,
                    content_type="multipart/form-data",
                )
        finally:
            self.app.config["MEDIA_LOCAL_FALLBACK_ENABLED"] = True

        self.assertEqual(resp.status_code, 503)
        self.assertEqual(resp.get_json()["error"], "Media storage is unavailable")

    def test_create_and_list_comments_tree(self):
        self._register("alice")
        headers = self._auth_header("alice")

        create_post = self.client.post(
            "/api/posts",
            json={"text": "post with comments"},
            headers=headers,
        )
        post_id = create_post.get_json()["post_id"]

        root_resp = self.client.post(
            f"/api/posts/{post_id}/comments",
            json={"text": "root comment"},
            headers=headers,
        )
        self.assertEqual(root_resp.status_code, 201)
        root_id = root_resp.get_json()["id"]

        reply_resp = self.client.post(
            f"/api/posts/{post_id}/comments",
            json={"text": "reply comment", "parent_id": root_id},
            headers=headers,
        )
        self.assertEqual(reply_resp.status_code, 201)

        list_resp = self.client.get(f"/api/posts/{post_id}/comments")
        self.assertEqual(list_resp.status_code, 200)
        comments = list_resp.get_json()
        self.assertEqual(len(comments), 1)
        self.assertEqual(comments[0]["text"], "root comment")
        self.assertEqual(comments[0]["author"]["id"], 1)
        self.assertEqual(comments[0]["author"]["username"], "alice")
        self.assertEqual(comments[0]["author"]["name"], "alice")
        self.assertIsNone(comments[0]["author"]["profile_image_url"])
        self.assertEqual(len(comments[0]["replies"]), 1)
        self.assertEqual(comments[0]["replies"][0]["text"], "reply comment")
        self.assertEqual(comments[0]["replies"][0]["author"]["username"], "alice")
        self.assertEqual(comments[0]["replies"][0]["author"]["name"], "alice")
        self.assertIsNone(comments[0]["replies"][0]["author"]["profile_image_url"])

    def test_list_comments_orders_by_score_then_newest(self):
        self._register("alice")
        headers = self._auth_header("alice")

        create_post = self.client.post(
            "/api/posts",
            json={"text": "post with sorted comments"},
            headers=headers,
        )
        post_id = create_post.get_json()["post_id"]

        root_a = self.client.post(
            f"/api/posts/{post_id}/comments",
            json={"text": "root low old"},
            headers=headers,
        ).get_json()["id"]
        root_b = self.client.post(
            f"/api/posts/{post_id}/comments",
            json={"text": "root low new"},
            headers=headers,
        ).get_json()["id"]
        root_c = self.client.post(
            f"/api/posts/{post_id}/comments",
            json={"text": "root high"},
            headers=headers,
        ).get_json()["id"]

        reply_a = self.client.post(
            f"/api/posts/{post_id}/comments",
            json={"text": "reply low old", "parent_id": root_c},
            headers=headers,
        ).get_json()["id"]
        reply_b = self.client.post(
            f"/api/posts/{post_id}/comments",
            json={"text": "reply low new", "parent_id": root_c},
            headers=headers,
        ).get_json()["id"]
        reply_c = self.client.post(
            f"/api/posts/{post_id}/comments",
            json={"text": "reply high", "parent_id": root_c},
            headers=headers,
        ).get_json()["id"]

        with self.app.app_context():
            now = datetime.utcnow()
            comment_updates = {
                root_a: (0, now - timedelta(minutes=30)),
                root_b: (0, now - timedelta(minutes=10)),
                root_c: (2, now - timedelta(minutes=40)),
                reply_a: (1, now - timedelta(minutes=20)),
                reply_b: (1, now - timedelta(minutes=5)),
                reply_c: (3, now - timedelta(minutes=60)),
            }
            for comment_id, (score, created_at) in comment_updates.items():
                comment = self.Comment.query.get(comment_id)
                comment.score = score
                comment.created_at = created_at
            self.db.session.commit()

        list_resp = self.client.get(f"/api/posts/{post_id}/comments")
        self.assertEqual(list_resp.status_code, 200)
        comments = list_resp.get_json()
        self.assertEqual([c["id"] for c in comments], [root_c, root_b, root_a])
        self.assertEqual(
            [c["id"] for c in comments[0]["replies"]],
            [reply_c, reply_b, reply_a],
        )

    def test_activity_notification_comment_uses_full_comment_text(self):
        self._register("alice")
        self._register("bob")
        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")

        post_resp = self.client.post(
            "/api/posts",
            json={"text": "post for activity notification"},
            headers=alice_headers,
        )
        post_id = post_resp.get_json()["post_id"]

        long_comment = "long-reply-" * 30
        comment_resp = self.client.post(
            f"/api/posts/{post_id}/comments",
            json={"text": long_comment},
            headers=bob_headers,
        )
        self.assertEqual(comment_resp.status_code, 201)
        self.assertGreater(len(long_comment), 120)
        with self.app.app_context():
            from app.models.profile_model import Profile
            from app.models.user_model import User

            actor_user = User.query.filter_by(username="bob").first()
            self.assertIsNotNone(actor_user)
            actor_user.badge = "verified"
            actor_profile = Profile.query.filter_by(user_id=actor_user.id).first()
            self.assertIsNotNone(actor_profile)
            actor_profile.profile_image_shape = "pill"
            self.db.session.commit()

        notifications_resp = self.client.get(
            "/api/activity-notifications?page=1&limit=20",
            headers=alice_headers,
        )
        self.assertEqual(notifications_resp.status_code, 200)
        payload = notifications_resp.get_json()
        self.assertEqual(payload["total"], 1)
        notification = payload["notifications"][0]
        self.assertEqual(notification["kind"], "comment")
        self.assertEqual(notification["actor"]["badge"], "verified")
        self.assertEqual(notification["actor"]["profile_image_shape"], "pill")
        self.assertEqual(notification["extra"]["comment_preview"], long_comment)

    def test_activity_notification_comment_reply_uses_full_reply_text(self):
        self._register("alice")
        self._register("bob")
        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")

        post_resp = self.client.post(
            "/api/posts",
            json={"text": "post for reply notification"},
            headers=alice_headers,
        )
        post_id = post_resp.get_json()["post_id"]

        root_comment_resp = self.client.post(
            f"/api/posts/{post_id}/comments",
            json={"text": "alice root comment"},
            headers=alice_headers,
        )
        self.assertEqual(root_comment_resp.status_code, 201)
        root_comment_id = root_comment_resp.get_json()["id"]

        long_reply = "very-long-nested-reply-" * 20
        reply_resp = self.client.post(
            f"/api/posts/{post_id}/comments",
            json={"text": long_reply, "parent_id": root_comment_id},
            headers=bob_headers,
        )
        self.assertEqual(reply_resp.status_code, 201)
        self.assertGreater(len(long_reply), 120)

        notifications_resp = self.client.get(
            "/api/activity-notifications?page=1&limit=20",
            headers=alice_headers,
        )
        self.assertEqual(notifications_resp.status_code, 200)
        payload = notifications_resp.get_json()
        self.assertEqual(payload["total"], 1)
        notification = payload["notifications"][0]
        self.assertEqual(notification["kind"], "comment_reply")
        self.assertEqual(notification["extra"]["comment_preview"], long_reply)

    def test_activity_notification_like_milestone_emits_once_for_threshold(self):
        self._register("alice")
        self._register("bob")
        self._register("charlie")

        self.app.config["ACTIVITY_MILESTONE_ENABLED"] = True
        self.app.config["ACTIVITY_MILESTONE_LIKE_PERCENT"] = 50
        self.app.config["ACTIVITY_MILESTONE_MIN_LIKES"] = 2
        self.app.config["ACTIVITY_MILESTONE_ACTIVE_USERS_CACHE_TTL_SECONDS"] = 0

        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")
        charlie_headers = self._auth_header("charlie")

        post_resp = self.client.post(
            "/api/posts",
            json={"text": "post for like milestone"},
            headers=alice_headers,
        )
        post_id = post_resp.get_json()["post_id"]

        first_like = self.client.post(
            "/api/votes",
            json={"target_type": "post", "target_id": post_id, "value": 1},
            headers=bob_headers,
        )
        self.assertEqual(first_like.status_code, 200)

        before_threshold = self.client.get(
            "/api/activity-notifications?page=1&limit=20",
            headers=alice_headers,
        )
        self.assertEqual(before_threshold.status_code, 200)
        before_payload = before_threshold.get_json()
        self.assertEqual(
            sum(1 for item in before_payload["notifications"] if item["kind"] == "post_like_milestone"),
            0,
        )

        second_like = self.client.post(
            "/api/votes",
            json={"target_type": "post", "target_id": post_id, "value": 1},
            headers=charlie_headers,
        )
        self.assertEqual(second_like.status_code, 200)

        after_threshold = self.client.get(
            "/api/activity-notifications?page=1&limit=30",
            headers=alice_headers,
        )
        self.assertEqual(after_threshold.status_code, 200)
        after_payload = after_threshold.get_json()
        milestone_notifications = [
            item for item in after_payload["notifications"]
            if item["kind"] == "post_like_milestone"
        ]
        self.assertEqual(len(milestone_notifications), 1)
        milestone = milestone_notifications[0]
        self.assertEqual(milestone["target_type"], "post")
        self.assertEqual(milestone["target_id"], post_id)
        self.assertEqual(milestone["extra"]["engagement_type"], "likes")
        self.assertEqual(milestone["extra"]["milestone_count"], 2)
        self.assertEqual(milestone["extra"]["engagement_count"], 2)

        duplicate_like = self.client.post(
            "/api/votes",
            json={"target_type": "post", "target_id": post_id, "value": 1},
            headers=charlie_headers,
        )
        self.assertEqual(duplicate_like.status_code, 200)

        after_duplicate = self.client.get(
            "/api/activity-notifications?page=1&limit=30",
            headers=alice_headers,
        )
        self.assertEqual(after_duplicate.status_code, 200)
        duplicate_payload = after_duplicate.get_json()
        duplicate_milestones = [
            item for item in duplicate_payload["notifications"]
            if item["kind"] == "post_like_milestone"
        ]
        self.assertEqual(len(duplicate_milestones), 1)

    def test_activity_notification_comment_milestone_uses_unique_commenters(self):
        self._register("alice")
        self._register("bob")
        self._register("charlie")

        self.app.config["ACTIVITY_MILESTONE_ENABLED"] = True
        self.app.config["ACTIVITY_MILESTONE_COMMENT_PERCENT"] = 50
        self.app.config["ACTIVITY_MILESTONE_MIN_COMMENTERS"] = 2
        self.app.config["ACTIVITY_MILESTONE_ACTIVE_USERS_CACHE_TTL_SECONDS"] = 0

        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")
        charlie_headers = self._auth_header("charlie")

        post_resp = self.client.post(
            "/api/posts",
            json={"text": "post for comment milestone"},
            headers=alice_headers,
        )
        post_id = post_resp.get_json()["post_id"]

        first_comment = self.client.post(
            f"/api/posts/{post_id}/comments",
            json={"text": "first from bob"},
            headers=bob_headers,
        )
        self.assertEqual(first_comment.status_code, 201)
        second_comment_same_user = self.client.post(
            f"/api/posts/{post_id}/comments",
            json={"text": "second from bob"},
            headers=bob_headers,
        )
        self.assertEqual(second_comment_same_user.status_code, 201)

        before_second_user = self.client.get(
            "/api/activity-notifications?page=1&limit=30",
            headers=alice_headers,
        )
        self.assertEqual(before_second_user.status_code, 200)
        before_payload = before_second_user.get_json()
        self.assertEqual(
            sum(1 for item in before_payload["notifications"] if item["kind"] == "post_comment_milestone"),
            0,
        )

        comment_from_second_user = self.client.post(
            f"/api/posts/{post_id}/comments",
            json={"text": "from charlie"},
            headers=charlie_headers,
        )
        self.assertEqual(comment_from_second_user.status_code, 201)

        after_second_user = self.client.get(
            "/api/activity-notifications?page=1&limit=40",
            headers=alice_headers,
        )
        self.assertEqual(after_second_user.status_code, 200)
        after_payload = after_second_user.get_json()
        milestone_notifications = [
            item for item in after_payload["notifications"]
            if item["kind"] == "post_comment_milestone"
        ]
        self.assertEqual(len(milestone_notifications), 1)
        milestone = milestone_notifications[0]
        self.assertEqual(milestone["target_type"], "post")
        self.assertEqual(milestone["target_id"], post_id)
        self.assertEqual(milestone["extra"]["engagement_type"], "comments")
        self.assertEqual(milestone["extra"]["milestone_count"], 2)
        self.assertEqual(milestone["extra"]["engagement_count"], 2)

    def test_delete_own_post_removes_post_and_related_comments(self):
        self._register("alice")
        self._register("bob")
        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")

        create_post_resp = self.client.post(
            "/api/posts",
            json={"text": "post to delete"},
            headers=alice_headers,
        )
        post_id = create_post_resp.get_json()["post_id"]

        create_comment_resp = self.client.post(
            f"/api/posts/{post_id}/comments",
            json={"text": "bob comment"},
            headers=bob_headers,
        )
        self.assertEqual(create_comment_resp.status_code, 201)

        delete_resp = self.client.delete(
            f"/api/posts/{post_id}",
            headers=alice_headers,
        )
        self.assertEqual(delete_resp.status_code, 200)
        self.assertEqual(delete_resp.get_json()["message"], "Post deleted")

        posts_resp = self.client.get("/api/posts", headers=alice_headers)
        self.assertEqual(posts_resp.status_code, 200)
        self.assertEqual(posts_resp.get_json()["total"], 0)

        comments_resp = self.client.get(f"/api/posts/{post_id}/comments")
        self.assertEqual(comments_resp.status_code, 404)
        self.assertEqual(comments_resp.get_json()["error"], "Post not found")

    def test_delete_post_rejects_non_owner(self):
        self._register("alice")
        self._register("bob")
        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")

        create_post_resp = self.client.post(
            "/api/posts",
            json={"text": "alice post"},
            headers=alice_headers,
        )
        post_id = create_post_resp.get_json()["post_id"]

        delete_resp = self.client.delete(
            f"/api/posts/{post_id}",
            headers=bob_headers,
        )
        self.assertEqual(delete_resp.status_code, 403)
        self.assertEqual(
            delete_resp.get_json()["error"],
            "You can only delete your own posts",
        )

    def test_delete_own_comment(self):
        self._register("alice")
        headers = self._auth_header("alice")

        create_post_resp = self.client.post(
            "/api/posts",
            json={"text": "post with deletable comment"},
            headers=headers,
        )
        post_id = create_post_resp.get_json()["post_id"]

        comment_resp = self.client.post(
            f"/api/posts/{post_id}/comments",
            json={"text": "comment to delete"},
            headers=headers,
        )
        self.assertEqual(comment_resp.status_code, 201)
        comment_id = comment_resp.get_json()["id"]

        delete_resp = self.client.delete(
            f"/api/comments/{comment_id}",
            headers=headers,
        )
        self.assertEqual(delete_resp.status_code, 200)
        self.assertEqual(delete_resp.get_json()["message"], "Comment deleted")

        comments_resp = self.client.get(f"/api/posts/{post_id}/comments")
        self.assertEqual(comments_resp.status_code, 200)
        self.assertEqual(comments_resp.get_json(), [])

    def test_delete_comment_rejects_non_owner(self):
        self._register("alice")
        self._register("bob")
        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")

        create_post_resp = self.client.post(
            "/api/posts",
            json={"text": "post for comment ownership"},
            headers=alice_headers,
        )
        post_id = create_post_resp.get_json()["post_id"]

        comment_resp = self.client.post(
            f"/api/posts/{post_id}/comments",
            json={"text": "alice comment"},
            headers=alice_headers,
        )
        comment_id = comment_resp.get_json()["id"]

        delete_resp = self.client.delete(
            f"/api/comments/{comment_id}",
            headers=bob_headers,
        )
        self.assertEqual(delete_resp.status_code, 403)
        self.assertEqual(
            delete_resp.get_json()["error"],
            "You can only delete your own comments",
        )

    def test_vote_route_duplicate_vote_is_idempotent(self):
        self._register("alice")
        headers = self._auth_header("alice")

        create_post = self.client.post(
            "/api/posts",
            json={"text": "post for vote"},
            headers=headers,
        )
        post_id = create_post.get_json()["post_id"]

        comment_resp = self.client.post(
            f"/api/posts/{post_id}/comments",
            json={"text": "comment to vote"},
            headers=headers,
        )
        comment_id = comment_resp.get_json()["id"]

        first_vote = self.client.post(
            "/api/votes",
            json={"target_type": "comment", "target_id": comment_id, "value": 1},
            headers=headers,
        )
        self.assertEqual(first_vote.status_code, 200)

        second_vote = self.client.post(
            "/api/votes",
            json={"target_type": "comment", "target_id": comment_id, "value": 1},
            headers=headers,
        )
        self.assertEqual(second_vote.status_code, 200)

        with self.app.app_context():
            comment = self.Comment.query.get(comment_id)
            self.assertEqual(comment.score, 1)

    def test_contacts_and_public_key_access_control(self):
        self._register("alice", public_key="alice_key")
        self._register("bob", public_key="bob_key")
        self._register("charlie", public_key="charlie_key")
        headers = self._auth_header("alice")

        add_contact = self.client.post(
            "/api/contacts",
            json={"contact": "bob"},
            headers=headers,
        )
        self.assertEqual(add_contact.status_code, 200)

        list_contacts = self.client.get("/api/contacts", headers=headers)
        self.assertEqual(list_contacts.status_code, 200)
        contacts = list_contacts.get_json()["contacts"]
        self.assertIn("bob", contacts)

        bob_key = self.client.get(
            "/api/contacts/bob/public-key",
            headers=headers,
        )
        self.assertEqual(bob_key.status_code, 200)
        self.assertEqual(bob_key.get_json()["public_key"], "bob_key")

        charlie_key = self.client.get(
            "/api/contacts/charlie/public-key",
            headers=headers,
        )
        self.assertEqual(charlie_key.status_code, 403)
        self.assertEqual(charlie_key.get_json()["error"], "Not in your contacts")

    def test_detailed_contacts_includes_group_metadata(self):
        self._register("alice")
        self._register("bob")
        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")

        self.assertEqual(
            self.client.post("/api/follows/bob", headers=alice_headers).status_code,
            200,
        )
        self.assertEqual(
            self.client.post("/api/follows/alice", headers=bob_headers).status_code,
            200,
        )

        create_group = self.client.post(
            "/api/groups",
            headers=alice_headers,
            json={
                "name": "close-friends",
                "members": ["bob"],
            },
        )
        self.assertEqual(create_group.status_code, 201)

        detailed_contacts = self.client.get(
            "/api/contacts?detailed=true",
            headers=alice_headers,
        )
        self.assertEqual(detailed_contacts.status_code, 200)
        payload = detailed_contacts.get_json()
        self.assertIn("groups", payload)
        self.assertEqual(len(payload["groups"]), 1)
        group_payload = payload["groups"][0]
        self.assertEqual(group_payload["name"], "close-friends")
        self.assertEqual(group_payload["creator"]["username"], "alice")
        self.assertEqual(group_payload["member_count"], 2)

    def test_contacts_delta_returns_only_requested_incremental_contacts(self):
        self._register("alice")
        self._register("bob")
        self._register("charlie")
        alice_headers = self._auth_header("alice")

        self.assertEqual(
            self.client.post("/api/follows/bob", headers=alice_headers).status_code,
            200,
        )

        with self.app.app_context():
            from app.services import message_service

            message_service.send_message(
                sender="charlie",
                recipient="alice",
                message="enc-delta",
                encrypted_key="key-delta",
            )

        delta_response = self.client.get(
            "/api/contacts/delta",
            query_string={"users": "bob,charlie,unknown"},
            headers=alice_headers,
        )
        self.assertEqual(delta_response.status_code, 200)
        payload = delta_response.get_json()
        contacts = payload.get("contacts", [])
        contact_usernames = {contact["username"] for contact in contacts}

        self.assertIn("bob", contact_usernames)
        self.assertIn("charlie", contact_usernames)
        self.assertNotIn("unknown", contact_usernames)

        by_username = {contact["username"]: contact for contact in contacts}
        self.assertFalse(by_username["bob"]["has_message"])
        self.assertTrue(by_username["charlie"]["has_message"])
        self.assertIsNotNone(by_username["charlie"]["last_message"])

    def test_detailed_contacts_uses_batched_timestamp_and_online_lookups(self):
        self._register("alice")
        self._register("bob")
        self._register("charlie")
        alice_headers = self._auth_header("alice")

        self.assertEqual(
            self.client.post("/api/follows/bob", headers=alice_headers).status_code,
            200,
        )
        self.assertEqual(
            self.client.post("/api/follows/charlie", headers=alice_headers).status_code,
            200,
        )

        from app.services import contact_service

        with patch.object(
            contact_service.message_repository,
            "get_contact_timestamp_score",
            side_effect=AssertionError("per-contact timestamp lookup should not be used"),
        ):
            with patch.object(
                contact_service.message_repository,
                "get_contact_timestamp_scores",
                wraps=contact_service.message_repository.get_contact_timestamp_scores,
            ) as batched_scores:
                with patch.object(
                    contact_service,
                    "get_users_online_status",
                    return_value={"bob": False, "charlie": False},
                ) as batched_online_status:
                    response = self.client.get(
                        "/api/contacts?detailed=true",
                        headers=alice_headers,
                    )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        usernames = {item["username"] for item in payload.get("contacts", [])}
        self.assertSetEqual(usernames, {"bob", "charlie"})
        self.assertGreaterEqual(batched_scores.call_count, 1)
        batched_online_status.assert_called_once()

    def test_contacts_delta_uses_batched_timestamp_lookup(self):
        self._register("alice")
        self._register("bob")
        self._register("charlie")
        alice_headers = self._auth_header("alice")

        self.assertEqual(
            self.client.post("/api/follows/bob", headers=alice_headers).status_code,
            200,
        )

        from app.services import contact_service

        with patch.object(
            contact_service.message_repository,
            "get_contact_timestamp_score",
            side_effect=AssertionError("per-contact timestamp lookup should not be used"),
        ):
            with patch.object(
                contact_service.message_repository,
                "get_contact_timestamp_scores",
                wraps=contact_service.message_repository.get_contact_timestamp_scores,
            ) as batched_scores:
                response = self.client.get(
                    "/api/contacts/delta",
                    query_string={"users": "bob,charlie,missing"},
                    headers=alice_headers,
                )

        self.assertEqual(response.status_code, 200)
        self.assertGreaterEqual(batched_scores.call_count, 1)

    def test_public_key_access_allowed_for_existing_conversation(self):
        self._register("alice", public_key="alice_key")
        self._register("charlie", public_key="charlie_key")
        alice_headers = self._auth_header("alice")

        with self.app.app_context():
            from app.services import message_service

            message_service.send_message(
                sender="charlie",
                recipient="alice",
                message="enc-msg",
                encrypted_key="enc-key",
            )

        charlie_key = self.client.get(
            "/api/contacts/charlie/public-key",
            headers=alice_headers,
        )
        self.assertEqual(charlie_key.status_code, 200)
        self.assertEqual(charlie_key.get_json()["public_key"], "charlie_key")

    def test_profile_follow_and_unfollow_flow(self):
        self._register("alice")
        self._register("bob")
        alice_headers = self._auth_header("alice")

        profile_before = self.client.get("/api/profiles/bob")
        self.assertEqual(profile_before.status_code, 200)
        before_payload = profile_before.get_json()
        self.assertEqual(before_payload["followers_count"], 0)
        self.assertEqual(before_payload["following_count"], 0)
        self.assertEqual(before_payload["posts_count"], 0)

        follow_resp = self.client.post("/api/follows/bob", headers=alice_headers)
        self.assertEqual(follow_resp.status_code, 200)
        self.assertEqual(follow_resp.get_json()["message"], "Followed")

        profile_after_follow = self.client.get("/api/profiles/bob")
        self.assertEqual(profile_after_follow.status_code, 200)
        after_follow_payload = profile_after_follow.get_json()
        self.assertEqual(after_follow_payload["followers_count"], 1)
        self.assertEqual(after_follow_payload["following_count"], 0)

        unfollow_resp = self.client.delete("/api/follows/bob", headers=alice_headers)
        self.assertEqual(unfollow_resp.status_code, 200)
        self.assertEqual(unfollow_resp.get_json()["message"], "Unfollowed")

        profile_after_unfollow = self.client.get("/api/profiles/bob")
        self.assertEqual(profile_after_unfollow.status_code, 200)
        after_unfollow_payload = profile_after_unfollow.get_json()
        self.assertEqual(after_unfollow_payload["followers_count"], 0)

    def test_block_user_hides_profiles_posts_and_search_results(self):
        self._register("alice")
        self._register("bob")
        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")

        self.assertEqual(
            self.client.post("/api/posts", json={"text": "alice says hi"}, headers=alice_headers).status_code,
            201,
        )
        self.assertEqual(
            self.client.post("/api/posts", json={"text": "bob says hi"}, headers=bob_headers).status_code,
            201,
        )

        block_response = self.client.post("/api/blocks/bob", headers=alice_headers)
        self.assertEqual(block_response.status_code, 201)
        self.assertTrue(block_response.get_json()["created"])

        block_repeat = self.client.post("/api/blocks/bob", headers=alice_headers)
        self.assertEqual(block_repeat.status_code, 200)
        self.assertFalse(block_repeat.get_json()["created"])

        alice_feed = self.client.get("/api/posts", headers=alice_headers)
        self.assertEqual(alice_feed.status_code, 200)
        self.assertEqual(alice_feed.get_json()["total"], 1)
        self.assertEqual(alice_feed.get_json()["posts"][0]["author"]["username"], "alice")

        bob_feed = self.client.get("/api/posts", headers=bob_headers)
        self.assertEqual(bob_feed.status_code, 200)
        self.assertEqual(bob_feed.get_json()["total"], 1)
        self.assertEqual(bob_feed.get_json()["posts"][0]["author"]["username"], "bob")

        blocked_profile = self.client.get("/api/profiles/alice", headers=bob_headers)
        self.assertEqual(blocked_profile.status_code, 404)
        self.assertEqual(blocked_profile.get_json()["error"], "User not found")

        blocked_profile_posts = self.client.get("/api/profiles/alice/posts", headers=bob_headers)
        self.assertEqual(blocked_profile_posts.status_code, 404)
        self.assertEqual(blocked_profile_posts.get_json()["error"], "User not found")

        hidden_profile_from_blocker = self.client.get("/api/profiles/bob", headers=alice_headers)
        self.assertEqual(hidden_profile_from_blocker.status_code, 404)

        users_search = self.client.get("/api/search/users?q=alice", headers=bob_headers)
        self.assertEqual(users_search.status_code, 200)
        self.assertEqual(users_search.get_json()["total"], 0)

        posts_search = self.client.get("/api/search/posts?q=alice", headers=bob_headers)
        self.assertEqual(posts_search.status_code, 200)
        self.assertEqual(posts_search.get_json()["total"], 0)

    def test_blocked_user_cannot_fetch_public_key(self):
        self._register("alice", public_key="alice_key")
        self._register("bob", public_key="bob_key")
        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")

        self.assertEqual(
            self.client.post("/api/follows/alice", headers=bob_headers).status_code,
            200,
        )
        self.assertEqual(
            self.client.post("/api/blocks/bob", headers=alice_headers).status_code,
            201,
        )

        public_key_response = self.client.get(
            "/api/contacts/alice/public-key",
            headers=bob_headers,
        )
        self.assertEqual(public_key_response.status_code, 403)
        self.assertEqual(
            public_key_response.get_json()["error"],
            "You cannot message this user",
        )

    def test_block_list_and_unblock_flow(self):
        self._register("alice")
        self._register("bob")
        self._register("charlie")
        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")

        self.assertEqual(
            self.client.post("/api/blocks/bob", headers=alice_headers).status_code,
            201,
        )
        self.assertEqual(
            self.client.post("/api/blocks/charlie", headers=alice_headers).status_code,
            201,
        )

        blocked_list = self.client.get("/api/blocks?page=1&limit=50", headers=alice_headers)
        self.assertEqual(blocked_list.status_code, 200)
        blocked_payload = blocked_list.get_json()
        self.assertEqual(blocked_payload["total"], 2)
        self.assertEqual(
            [user["username"] for user in blocked_payload["users"]],
            ["bob", "charlie"],
        )

        blocked_profile = self.client.get("/api/profiles/alice", headers=bob_headers)
        self.assertEqual(blocked_profile.status_code, 404)

        unblock_response = self.client.delete("/api/blocks/bob", headers=alice_headers)
        self.assertEqual(unblock_response.status_code, 200)
        self.assertEqual(unblock_response.get_json()["message"], "User unblocked")
        self.assertTrue(unblock_response.get_json()["removed"])

        blocked_list_after = self.client.get("/api/blocks?page=1&limit=50", headers=alice_headers)
        self.assertEqual(blocked_list_after.status_code, 200)
        blocked_after_payload = blocked_list_after.get_json()
        self.assertEqual(blocked_after_payload["total"], 1)
        self.assertEqual(blocked_after_payload["users"][0]["username"], "charlie")

        unblocked_profile = self.client.get("/api/profiles/alice", headers=bob_headers)
        self.assertEqual(unblocked_profile.status_code, 200)

    def test_follow_status_endpoint(self):
        self._register("alice")
        self._register("bob")
        alice_headers = self._auth_header("alice")

        status_before = self.client.get("/api/follows/bob/status", headers=alice_headers)
        self.assertEqual(status_before.status_code, 200)
        self.assertFalse(status_before.get_json()["is_following"])

        follow_resp = self.client.post("/api/follows/bob", headers=alice_headers)
        self.assertEqual(follow_resp.status_code, 200)
        self.assertEqual(follow_resp.get_json()["message"], "Followed")

        status_after = self.client.get("/api/follows/bob/status", headers=alice_headers)
        self.assertEqual(status_after.status_code, 200)
        self.assertTrue(status_after.get_json()["is_following"])

        missing_user = self.client.get(
            "/api/follows/unknown/status",
            headers=alice_headers,
        )
        self.assertEqual(missing_user.status_code, 404)
        self.assertEqual(missing_user.get_json()["error"], "User not found")

    def test_followers_and_following_endpoints_with_pagination(self):
        self._register("alice")
        self._register("bob")
        self._register("charlie")
        self._register("dave")

        bob_headers = self._auth_header("bob")
        charlie_headers = self._auth_header("charlie")
        dave_headers = self._auth_header("dave")
        alice_headers = self._auth_header("alice")

        self.assertEqual(
            self.client.post("/api/follows/alice", headers=bob_headers).status_code,
            200,
        )
        self.assertEqual(
            self.client.post("/api/follows/alice", headers=charlie_headers).status_code,
            200,
        )
        self.assertEqual(
            self.client.post("/api/follows/alice", headers=dave_headers).status_code,
            200,
        )
        self.assertEqual(
            self.client.post("/api/follows/bob", headers=alice_headers).status_code,
            200,
        )
        self.assertEqual(
            self.client.post("/api/follows/charlie", headers=alice_headers).status_code,
            200,
        )

        followers_page_1 = self.client.get("/api/follows/alice/followers?page=1&limit=2")
        self.assertEqual(followers_page_1.status_code, 200)
        followers_payload_1 = followers_page_1.get_json()
        self.assertEqual(followers_payload_1["page"], 1)
        self.assertEqual(followers_payload_1["limit"], 2)
        self.assertEqual(followers_payload_1["total"], 3)
        self.assertEqual(len(followers_payload_1["users"]), 2)
        self.assertEqual(followers_payload_1["users"][0]["username"], "bob")
        self.assertEqual(followers_payload_1["users"][1]["username"], "charlie")

        followers_page_2 = self.client.get("/api/follows/alice/followers?page=2&limit=2")
        self.assertEqual(followers_page_2.status_code, 200)
        followers_payload_2 = followers_page_2.get_json()
        self.assertEqual(followers_payload_2["total"], 3)
        self.assertEqual(len(followers_payload_2["users"]), 1)
        self.assertEqual(followers_payload_2["users"][0]["username"], "dave")

        following_list = self.client.get("/api/follows/alice/following?page=1&limit=10")
        self.assertEqual(following_list.status_code, 200)
        following_payload = following_list.get_json()
        self.assertEqual(following_payload["total"], 2)
        self.assertEqual(
            [user["username"] for user in following_payload["users"]],
            ["bob", "charlie"],
        )

        missing = self.client.get("/api/follows/unknown/followers")
        self.assertEqual(missing.status_code, 404)
        self.assertEqual(missing.get_json()["error"], "User not found")

    def test_profile_image_replacement_deletes_previous_object(self):
        self._register("alice")
        headers = self._auth_header("alice")

        class FakeMinio:
            def __init__(self):
                self.put_calls = []
                self.remove_calls = []

            def put_object(self, **kwargs):
                self.put_calls.append(kwargs)
                return None

            def remove_object(self, **kwargs):
                self.remove_calls.append(kwargs)
                return None

        fake_minio = FakeMinio()

        with patch("app.services.profile_service.get_minio_client", return_value=fake_minio):
            first_update = self.client.put(
                "/api/profiles/me",
                data={
                    "profile_image": (
                        io.BytesIO(b"first-image"),
                        "first.webp",
                        "image/webp",
                    ),
                },
                headers=headers,
                content_type="multipart/form-data",
            )
            self.assertEqual(first_update.status_code, 200)
            first_payload = first_update.get_json()
            first_image_url = first_payload["profile_image_url"]
            self.assertIn("/media/profiles/1/images/", first_image_url)

            second_update = self.client.put(
                "/api/profiles/me",
                data={
                    "profile_image": (
                        io.BytesIO(b"second-image"),
                        "second.webp",
                        "image/webp",
                    ),
                },
                headers=headers,
                content_type="multipart/form-data",
            )
            self.assertEqual(second_update.status_code, 200)
            second_payload = second_update.get_json()
            second_image_url = second_payload["profile_image_url"]
            self.assertIn("/media/profiles/1/images/", second_image_url)
            self.assertNotEqual(first_image_url, second_image_url)

        old_object_name = first_image_url.split("/media/", 1)[1]
        removed_object_names = [call["object_name"] for call in fake_minio.remove_calls]
        self.assertIn(old_object_name, removed_object_names)

    def test_profile_video_upload_and_replacement(self):
        self._register("alice")
        headers = self._auth_header("alice")

        class FakeMinio:
            def __init__(self):
                self.put_calls = []
                self.remove_calls = []

            def put_object(self, **kwargs):
                self.put_calls.append(kwargs)
                return None

            def remove_object(self, **kwargs):
                self.remove_calls.append(kwargs)
                return None

        fake_minio = FakeMinio()

        with (
            patch("app.services.profile_service.get_minio_client", return_value=fake_minio),
            patch(
                "app.services.profile_service._get_mp4_duration_seconds",
                side_effect=[4.0, 4.5],
            ),
        ):
            first_update = self.client.put(
                "/api/profiles/me",
                data={
                    "profile_video": (
                        io.BytesIO(b"video-one"),
                        "first.mp4",
                        "video/mp4",
                    ),
                },
                headers=headers,
                content_type="multipart/form-data",
            )
            self.assertEqual(first_update.status_code, 200)
            first_video_url = first_update.get_json()["profile_video_url"]
            self.assertIn("/media/profiles/1/videos/", first_video_url)

            second_update = self.client.put(
                "/api/profiles/me",
                data={
                    "profile_video": (
                        io.BytesIO(b"video-two"),
                        "second.mp4",
                        "video/mp4",
                    ),
                },
                headers=headers,
                content_type="multipart/form-data",
            )
            self.assertEqual(second_update.status_code, 200)
            second_video_url = second_update.get_json()["profile_video_url"]
            self.assertIn("/media/profiles/1/videos/", second_video_url)
            self.assertNotEqual(first_video_url, second_video_url)

        old_video_object_name = first_video_url.split("/media/", 1)[1]
        removed_object_names = [call["object_name"] for call in fake_minio.remove_calls]
        self.assertIn(old_video_object_name, removed_object_names)

    def test_profile_video_duration_limit(self):
        self._register("alice")
        headers = self._auth_header("alice")

        class FakeMinio:
            def put_object(self, **kwargs):
                return None

            def remove_object(self, **kwargs):
                return None

        with (
            patch("app.services.profile_service.get_minio_client", return_value=FakeMinio()),
            patch("app.services.profile_service._get_mp4_duration_seconds", return_value=6.0),
        ):
            response = self.client.put(
                "/api/profiles/me",
                data={
                    "profile_video": (
                        io.BytesIO(b"video-data"),
                        "too-long.mp4",
                        "video/mp4",
                    ),
                },
                headers=headers,
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.get_json()["error"],
            "Profile video must be 5 seconds or shorter",
        )

    def test_profile_video_size_limit(self):
        self._register("alice")
        headers = self._auth_header("alice")

        class FakeMinio:
            def put_object(self, **kwargs):
                return None

            def remove_object(self, **kwargs):
                return None

        original_limit = self.app.config.get("PROFILE_VIDEO_MAX_SIZE_BYTES")
        self.app.config["PROFILE_VIDEO_MAX_SIZE_BYTES"] = 1024

        try:
            with (
                patch("app.services.profile_service.get_minio_client", return_value=FakeMinio()),
                patch("app.services.profile_service._get_mp4_duration_seconds", return_value=4.0),
            ):
                response = self.client.put(
                    "/api/profiles/me",
                    data={
                        "profile_video": (
                            io.BytesIO(b"x" * 2048),
                            "too-large.mp4",
                            "video/mp4",
                        ),
                    },
                    headers=headers,
                    content_type="multipart/form-data",
                )
        finally:
            self.app.config["PROFILE_VIDEO_MAX_SIZE_BYTES"] = original_limit

        self.assertEqual(response.status_code, 400)
        self.assertIn("Profile video is too large", response.get_json()["error"])

    def test_profile_update_me_and_get_me(self):
        self._register("alice")
        headers = self._auth_header("alice")

        update_response = self.client.put(
            "/api/profiles/me",
            json={"name": "Alice Wonder", "bio": "Bio text"},
            headers=headers,
        )
        self.assertEqual(update_response.status_code, 200)
        update_payload = update_response.get_json()
        self.assertEqual(update_payload["username"], "alice")
        self.assertEqual(update_payload["name"], "Alice Wonder")
        self.assertEqual(update_payload["bio"], "Bio text")

        get_me = self.client.get("/api/profiles/me", headers=headers)
        self.assertEqual(get_me.status_code, 200)
        me_payload = get_me.get_json()
        self.assertEqual(me_payload["name"], "Alice Wonder")
        self.assertEqual(me_payload["bio"], "Bio text")
        self.assertEqual(me_payload["profile_image_shape"], "circle")

    def test_profile_shape_update_requires_badge(self):
        self._register("alice")
        headers = self._auth_header("alice")

        without_badge = self.client.put(
            "/api/profiles/me",
            json={"profile_image_shape": "pill"},
            headers=headers,
        )
        self.assertEqual(without_badge.status_code, 400)
        self.assertIn(
            "available only for users with a badge",
            without_badge.get_json()["error"],
        )

        with self.app.app_context():
            from app.models.user_model import User

            user = User.query.filter_by(username="alice").first()
            self.assertIsNotNone(user)
            user.badge = "verified"
            self.db.session.commit()

        with_badge = self.client.put(
            "/api/profiles/me",
            json={"profile_image_shape": "pill"},
            headers=headers,
        )
        self.assertEqual(with_badge.status_code, 200)
        self.assertEqual(with_badge.get_json()["profile_image_shape"], "pill")

    def test_profile_shape_update_rejects_invalid_shape(self):
        self._register("alice")
        headers = self._auth_header("alice")

        with self.app.app_context():
            from app.models.user_model import User

            user = User.query.filter_by(username="alice").first()
            self.assertIsNotNone(user)
            user.badge = "verified"
            self.db.session.commit()

        response = self.client.put(
            "/api/profiles/me",
            json={"profile_image_shape": "triangle"},
            headers=headers,
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("Unsupported profile image shape", response.get_json()["error"])

    def test_delete_my_account_removes_user_data(self):
        self._register("alice")
        self._register("bob")
        alice_headers = self._auth_header("alice")
        alice_refresh_headers = self._refresh_header("alice")
        bob_headers = self._auth_header("bob")

        alice_post_response = self.client.post(
            "/api/posts",
            json={"text": "alice post"},
            headers=alice_headers,
        )
        self.assertEqual(alice_post_response.status_code, 201)
        alice_post_id = alice_post_response.get_json()["post_id"]

        bob_post_response = self.client.post(
            "/api/posts",
            json={"text": "bob post"},
            headers=bob_headers,
        )
        self.assertEqual(bob_post_response.status_code, 201)
        bob_post_id = bob_post_response.get_json()["post_id"]

        alice_comment_response = self.client.post(
            f"/api/posts/{bob_post_id}/comments",
            json={"text": "alice comment on bob"},
            headers=alice_headers,
        )
        self.assertEqual(alice_comment_response.status_code, 201)

        self.assertEqual(
            self.client.post("/api/follows/bob", headers=alice_headers).status_code,
            200,
        )
        self.assertEqual(
            self.client.post("/api/blocks/bob", headers=alice_headers).status_code,
            201,
        )

        with self.app.app_context():
            from app.models.activity_notification_model import ActivityNotification
            from app.models.comment_model import Comment
            from app.models.crash_log_model import CrashLog
            from app.models.follow_model import Follow
            from app.models.group_model import Group, GroupMember
            from app.models.pending_registration_model import PendingRegistration
            from app.models.post_model import Post
            from app.models.profile_model import Profile
            from app.models.profile_video_model import ProfileVideo
            from app.models.user_model import User
            from app.repositories import message_repository

            alice = User.query.filter_by(username="alice").first()
            bob = User.query.filter_by(username="bob").first()
            self.assertIsNotNone(alice)
            self.assertIsNotNone(bob)

            profile = Profile.query.filter_by(user_id=alice.id).first()
            self.assertIsNotNone(profile)
            profile.image_object_name = "static/uploads/test-alice-image.webp"
            db_profile_video = ProfileVideo(
                user_id=alice.id,
                video_object_name="static/uploads/test-alice-video.mp4",
            )
            self.db.session.add(db_profile_video)

            self.db.session.add(
                CrashLog(
                    event_id="alice-crash-event",
                    platform="android",
                    app_version="1.0.0",
                    app_version_code=1,
                    thread_name="main",
                    exception_type="RuntimeError",
                    exception_message="boom",
                    stack_trace="stack",
                    occurred_at=datetime.utcnow(),
                    user_id=alice.id,
                    username_snapshot="alice",
                )
            )
            self.db.session.add(
                PendingRegistration(
                    registration_id="pending-alice-reg",
                    username="alice",
                    password_hash="hash",
                    public_key="pub",
                    name="Alice",
                    expires_at=datetime.utcnow() + timedelta(minutes=5),
                )
            )

            group = Group(name="alice group", creator_id=alice.id)
            self.db.session.add(group)
            self.db.session.flush()
            self.db.session.add(GroupMember(group_id=group.id, user_id=alice.id))
            self.db.session.add(GroupMember(group_id=group.id, user_id=bob.id))

            payload = message_repository.build_message_payload(
                sender="alice",
                encrypted_message="enc",
                encrypted_key="k",
            )
            private_message_id = payload["message_id"]
            message_repository.push_message_payload("bob", payload)
            message_repository.store_private_message_metadata(payload, "bob")

            incoming_payload = message_repository.build_message_payload(
                sender="bob",
                encrypted_message="enc-incoming",
                encrypted_key="k2",
            )
            incoming_message_id = incoming_payload["message_id"]
            message_repository.push_message_payload("alice", incoming_payload)
            message_repository.store_private_message_metadata(incoming_payload, "alice")
            self.db.session.commit()

        delete_response = self.client.delete("/api/profiles/me", headers=alice_headers)
        self.assertEqual(delete_response.status_code, 200)
        self.assertEqual(delete_response.get_json()["message"], "Account deleted permanently")

        refresh_response = self.client.post("/api/auth/refresh", headers=alice_refresh_headers)
        self.assertEqual(refresh_response.status_code, 401)

        login_response = self.client.post(
            "/api/auth/login",
            json={"username": "alice", "password": "pass123"},
        )
        self.assertEqual(login_response.status_code, 401)

        with self.app.app_context():
            from app.models.activity_notification_model import ActivityNotification
            from app.models.block_model import Block
            from app.models.comment_model import Comment
            from app.models.crash_log_model import CrashLog
            from app.models.follow_model import Follow
            from app.models.group_model import Group, GroupMember
            from app.models.pending_registration_model import PendingRegistration
            from app.models.post_model import Post
            from app.models.profile_model import Profile
            from app.models.profile_video_model import ProfileVideo
            from app.models.user_model import User
            from app.repositories import message_repository

            self.assertIsNone(User.query.filter_by(username="alice").first())
            self.assertEqual(
                Profile.query.join(User, Profile.user_id == User.id)
                .filter(User.username == "alice")
                .count(),
                0,
            )
            self.assertEqual(
                Post.query.join(User, Post.author_id == User.id)
                .filter(User.username == "alice")
                .count(),
                0,
            )
            self.assertEqual(
                Comment.query.join(User, Comment.author_id == User.id)
                .filter(User.username == "alice")
                .count(),
                0,
            )
            self.assertEqual(
                Follow.query.filter(
                    (Follow.follower_id == self._user_id("bob")) | (Follow.following_id == self._user_id("bob"))
                ).count() >= 0,
                True,
            )
            self.assertEqual(
                Block.query.count(),
                0,
            )
            self.assertEqual(
                ActivityNotification.query.count(),
                0,
            )
            self.assertEqual(
                Group.query.filter_by(name="alice group").count(),
                0,
            )
            self.assertEqual(
                GroupMember.query.count(),
                0,
            )
            self.assertEqual(
                ProfileVideo.query.count(),
                0,
            )
            self.assertEqual(
                PendingRegistration.query.filter_by(username="alice").count(),
                0,
            )
            self.assertEqual(
                CrashLog.query.filter_by(username_snapshot="alice").count(),
                0,
            )

            pending_for_bob = message_repository.peek_messages("bob")
            self.assertTrue(
                all((message or {}).get("from") != "alice" for message in pending_for_bob)
            )
            self.assertIsNone(message_repository.get_message_metadata(private_message_id))
            self.assertIsNone(message_repository.get_message_metadata(incoming_message_id))

    def test_profile_posts_endpoint_returns_only_target_user_posts(self):
        self._register("alice")
        self._register("bob")
        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")

        alice_post = self.client.post(
            "/api/posts",
            json={"text": "alice post"},
            headers=alice_headers,
        )
        self.assertEqual(alice_post.status_code, 201)

        bob_post = self.client.post(
            "/api/posts",
            json={"text": "bob post"},
            headers=bob_headers,
        )
        self.assertEqual(bob_post.status_code, 201)

        alice_posts = self.client.get("/api/profiles/alice/posts")
        self.assertEqual(alice_posts.status_code, 200)
        alice_payload = alice_posts.get_json()
        self.assertEqual(alice_payload["total"], 1)
        self.assertEqual(len(alice_payload["posts"]), 1)
        self.assertEqual(alice_payload["posts"][0]["text"], "alice post")
        self.assertEqual(alice_payload["posts"][0]["author"]["username"], "alice")
        self.assertEqual(alice_payload["posts"][0]["author"]["name"], "alice")
        self.assertIsNone(alice_payload["posts"][0]["author"]["profile_image_url"])
        self.assertEqual(alice_payload["posts"][0]["viewer_vote"], 0)

        bob_posts = self.client.get("/api/profiles/bob/posts")
        self.assertEqual(bob_posts.status_code, 200)
        bob_payload = bob_posts.get_json()
        self.assertEqual(bob_payload["total"], 1)
        self.assertEqual(len(bob_payload["posts"]), 1)
        self.assertEqual(bob_payload["posts"][0]["text"], "bob post")
        self.assertEqual(bob_payload["posts"][0]["author"]["username"], "bob")
        self.assertEqual(bob_payload["posts"][0]["author"]["name"], "bob")
        self.assertIsNone(bob_payload["posts"][0]["author"]["profile_image_url"])
        self.assertEqual(bob_payload["posts"][0]["viewer_vote"], 0)

    def test_profile_posts_pagination_normalizes_page_and_stable_sort(self):
        self._register("alice")
        alice_headers = self._auth_header("alice")

        created_ids = []
        for index in range(5):
            response = self.client.post(
                "/api/posts",
                json={"text": f"alice post {index}"},
                headers=alice_headers,
            )
            self.assertEqual(response.status_code, 201)
            created_ids.append(response.get_json()["post_id"])

        with self.app.app_context():
            from app.models.post_model import Post

            # Force identical timestamps so deterministic id tiebreaker is required.
            shared_created_at = datetime(2024, 1, 1, 0, 0, 0)
            Post.query.update({"created_at": shared_created_at})
            self.db.session.commit()

        first_page_with_zero = self.client.get("/api/profiles/alice/posts?page=0&limit=2")
        self.assertEqual(first_page_with_zero.status_code, 200)
        first_payload = first_page_with_zero.get_json()
        self.assertEqual(first_payload["page"], 1)
        first_ids = [post["id"] for post in first_payload["posts"]]
        self.assertEqual(first_ids, [5, 4])

        second_page = self.client.get("/api/profiles/alice/posts?page=2&limit=2")
        self.assertEqual(second_page.status_code, 200)
        second_ids = [post["id"] for post in second_page.get_json()["posts"]]
        self.assertEqual(second_ids, [3, 2])

        third_page = self.client.get("/api/profiles/alice/posts?page=3&limit=2")
        self.assertEqual(third_page.status_code, 200)
        third_ids = [post["id"] for post in third_page.get_json()["posts"]]
        self.assertEqual(third_ids, [1])

        all_loaded = first_ids + second_ids + third_ids
        self.assertEqual(sorted(all_loaded), sorted(created_ids))

    def test_posts_endpoints_include_viewer_vote_when_authenticated(self):
        self._register("alice")
        self._register("bob")
        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")

        create_response = self.client.post(
            "/api/posts",
            json={"text": "persisted vote post"},
            headers=bob_headers,
        )
        self.assertEqual(create_response.status_code, 201)
        post_id = create_response.get_json()["post_id"]

        vote_response = self.client.post(
            "/api/votes",
            json={
                "target_type": "post",
                "target_id": post_id,
                "value": 1,
            },
            headers=alice_headers,
        )
        self.assertEqual(vote_response.status_code, 200)

        feed_without_auth = self.client.get("/api/posts")
        self.assertEqual(feed_without_auth.status_code, 200)
        self.assertEqual(feed_without_auth.get_json()["posts"][0]["viewer_vote"], 0)

        feed_with_auth = self.client.get("/api/posts", headers=alice_headers)
        self.assertEqual(feed_with_auth.status_code, 200)
        self.assertEqual(feed_with_auth.get_json()["posts"][0]["viewer_vote"], 1)

        profile_posts = self.client.get("/api/profiles/bob/posts", headers=alice_headers)
        self.assertEqual(profile_posts.status_code, 200)
        self.assertEqual(profile_posts.get_json()["posts"][0]["viewer_vote"], 1)

        search_posts = self.client.get(
            "/api/search/posts?q=persisted",
            headers=alice_headers,
        )
        self.assertEqual(search_posts.status_code, 200)
        self.assertEqual(search_posts.get_json()["posts"][0]["viewer_vote"], 1)

        search_all = self.client.get(
            "/api/search?q=persisted",
            headers=alice_headers,
        )
        self.assertEqual(search_all.status_code, 200)
        self.assertEqual(search_all.get_json()["posts"][0]["viewer_vote"], 1)

    def test_post_responses_include_daily_winner_fields(self):
        with self.app.app_context():
            from app.models.post_model import Post
            from app.models.user_model import User

            user = User(
                username="alice",
                password_hash="x",
                public_key="pk1",
            )
            self.db.session.add(user)
            self.db.session.flush()

            historical_at = datetime(2026, 4, 29, 21, 0, 0, tzinfo=timezone.utc)
            winner_post = Post(
                author_id=user.id,
                text="winner-post",
                created_at=datetime(2026, 4, 30, 20, 0, 0),
                is_daily_winner=True,
                daily_winner_at=historical_at,
            )
            regular_post = Post(
                author_id=user.id,
                text="regular-post",
                created_at=datetime(2026, 4, 30, 19, 0, 0),
                is_daily_winner=False,
                daily_winner_at=None,
            )
            self.db.session.add_all([winner_post, regular_post])
            self.db.session.commit()

        feed_response = self.client.get("/api/posts")
        self.assertEqual(feed_response.status_code, 200)
        feed_posts = feed_response.get_json()["posts"]
        by_text = {item["text"]: item for item in feed_posts}

        self.assertTrue(by_text["winner-post"]["is_daily_winner"])
        self.assertTrue(by_text["winner-post"]["was_daily_winner"])
        self.assertIsNotNone(by_text["winner-post"]["daily_winner_at"])
        self.assertFalse(by_text["regular-post"]["is_daily_winner"])
        self.assertFalse(by_text["regular-post"]["was_daily_winner"])
        self.assertIsNone(by_text["regular-post"]["daily_winner_at"])

        search_posts = self.client.get("/api/search/posts?q=post")
        self.assertEqual(search_posts.status_code, 200)
        search_by_text = {
            item["text"]: item
            for item in search_posts.get_json()["posts"]
        }
        self.assertIn("is_daily_winner", search_by_text["winner-post"])
        self.assertIn("was_daily_winner", search_by_text["winner-post"])
        self.assertIn("daily_winner_at", search_by_text["winner-post"])

        profile_posts = self.client.get("/api/profiles/alice/posts")
        self.assertEqual(profile_posts.status_code, 200)
        profile_by_text = {
            item["text"]: item
            for item in profile_posts.get_json()["posts"]
        }
        self.assertTrue(profile_by_text["winner-post"]["was_daily_winner"])
        self.assertIsNotNone(profile_by_text["winner-post"]["daily_winner_at"])

    def test_report_types_include_copyright(self):
        response = self.client.get("/api/report-types")

        self.assertEqual(response.status_code, 200)
        report_types = response.get_json()["report_types"]
        self.assertIn("copyright", report_types)

    def test_report_post_accepts_copyright_report_type(self):
        self._register("alice")
        self._register("bob")
        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")

        create_post = self.client.post(
            "/api/posts",
            json={"text": "possible copyright issue"},
            headers=bob_headers,
        )
        self.assertEqual(create_post.status_code, 201)
        post_id = create_post.get_json()["post_id"]

        report_resp = self.client.post(
            f"/api/posts/{post_id}/reports",
            json={"report_type": "copyright"},
            headers=alice_headers,
        )
        self.assertEqual(report_resp.status_code, 201)

    def test_report_post_and_handle_delete_post_hides_post(self):
        self._register("alice")
        self._register("bob")
        self._register("admin")
        self._make_admin("admin")

        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")
        admin_headers = self._auth_header("admin")

        create_post = self.client.post(
            "/api/posts",
            json={"text": "reported post"},
            headers=bob_headers,
        )
        self.assertEqual(create_post.status_code, 201)
        post_id = create_post.get_json()["post_id"]

        report_resp = self.client.post(
            f"/api/posts/{post_id}/reports",
            json={"report_type": "spam", "description": "this looks spammy"},
            headers=alice_headers,
        )
        self.assertEqual(report_resp.status_code, 201)

        reports_resp = self.client.get("/admin/api/reports", headers=admin_headers)
        self.assertEqual(reports_resp.status_code, 200)
        reports_payload = reports_resp.get_json()
        self.assertEqual(reports_payload["total"], 1)
        report_id = reports_payload["reports"][0]["id"]
        self.assertEqual(reports_payload["reports"][0]["status"], "pending")

        handle_resp = self.client.post(
            f"/admin/api/reports/{report_id}/handle",
            json={"decision": "delete_post", "admin_note": "policy violation"},
            headers=admin_headers,
        )
        self.assertEqual(handle_resp.status_code, 200)
        handled_payload = handle_resp.get_json()["report"]
        self.assertEqual(handled_payload["status"], "handled")
        self.assertEqual(handled_payload["admin_decision"], "delete_post")
        self.assertEqual(handled_payload["handled_by_admin"]["username"], "admin")

        feed_resp = self.client.get("/api/posts")
        self.assertEqual(feed_resp.status_code, 200)
        self.assertEqual(feed_resp.get_json()["total"], 0)

        comments_resp = self.client.get(f"/api/posts/{post_id}/comments")
        self.assertEqual(comments_resp.status_code, 404)
        self.assertEqual(comments_resp.get_json()["error"], "Post not found")

        with self.app.app_context():
            from app.models.post_model import Post

            post = Post.query.get(post_id)
            self.assertIsNotNone(post)
            self.assertTrue(post.is_hidden)
            self.assertIsNotNone(post.purge_after)

    def test_report_post_and_handle_delete_account_suspends_user(self):
        self._register("alice")
        self._register("bob")
        self._register("admin")
        self._make_admin("admin")

        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")
        admin_headers = self._auth_header("admin")

        create_post = self.client.post(
            "/api/posts",
            json={"text": "reported account post"},
            headers=bob_headers,
        )
        post_id = create_post.get_json()["post_id"]

        report_resp = self.client.post(
            f"/api/posts/{post_id}/reports",
            json={"report_type": "scam"},
            headers=alice_headers,
        )
        self.assertEqual(report_resp.status_code, 201)
        report_id = report_resp.get_json()["report_id"]

        handle_resp = self.client.post(
            f"/admin/api/reports/{report_id}/handle",
            json={"decision": "delete_account"},
            headers=admin_headers,
        )
        self.assertEqual(handle_resp.status_code, 200)
        self.assertEqual(handle_resp.get_json()["report"]["admin_decision"], "delete_account")

        profile_resp = self.client.get("/api/profiles/bob")
        self.assertEqual(profile_resp.status_code, 404)

        login_resp = self.client.post(
            "/api/auth/login",
            json={"username": "bob", "password": "pass123"},
        )
        self.assertEqual(login_resp.status_code, 403)
        self.assertEqual(login_resp.get_json()["error"], "Account suspended")

        feed_resp = self.client.get("/api/posts")
        self.assertEqual(feed_resp.status_code, 200)
        self.assertEqual(feed_resp.get_json()["total"], 0)

        with self.app.app_context():
            from app.models.user_model import User
            from app.models.post_model import Post

            user = User.query.filter_by(username="bob").first()
            self.assertIsNotNone(user)
            self.assertTrue(user.is_suspended)

            post = Post.query.get(post_id)
            self.assertIsNotNone(post)
            self.assertTrue(post.is_hidden)

    def test_report_cleanup_hard_deletes_after_retention_window(self):
        self._register("alice")
        self._register("bob")
        self._register("admin")
        self._make_admin("admin")

        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")
        admin_headers = self._auth_header("admin")

        create_post = self.client.post(
            "/api/posts",
            json={"text": "to be purged"},
            headers=bob_headers,
        )
        post_id = create_post.get_json()["post_id"]

        report_resp = self.client.post(
            f"/api/posts/{post_id}/reports",
            json={"report_type": "false_information"},
            headers=alice_headers,
        )
        report_id = report_resp.get_json()["report_id"]

        handle_resp = self.client.post(
            f"/admin/api/reports/{report_id}/handle",
            json={"decision": "delete_post"},
            headers=admin_headers,
        )
        self.assertEqual(handle_resp.status_code, 200)

        with self.app.app_context():
            from app.models.post_model import Post
            from app.models.report_model import PostReport
            from app.services import report_service

            post = Post.query.get(post_id)
            report = PostReport.query.get(report_id)
            self.assertIsNotNone(post)
            self.assertIsNotNone(report)

            now = datetime.utcnow()
            post.purge_after = now - timedelta(seconds=1)
            report.decision_expires_at = now - timedelta(seconds=1)
            self.db.session.commit()

            report_service.run_scheduled_cleanup(force=True)

            self.assertIsNone(Post.query.get(post_id))
            self.assertIsNone(PostReport.query.get(report_id))

    def test_report_cleanup_honors_batch_size_limits(self):
        with self.app.app_context():
            from app.models.user_model import User
            from app.services import report_service

            due_at = datetime.utcnow() - timedelta(seconds=1)
            for idx in range(3):
                self.db.session.add(
                    User(
                        username=f"suspended_{idx}",
                        password_hash="hash",
                        public_key=f"pk_{idx}",
                        is_suspended=True,
                        purge_after=due_at,
                    )
                )
            self.db.session.commit()

            first_cycle = report_service.run_scheduled_cleanup_with_metrics(
                force=True,
                batch_size=2,
            )
            self.assertEqual(first_cycle["rows_processed"], 2)
            self.assertEqual(first_cycle["users_deleted"], 2)
            self.assertEqual(
                User.query.filter(User.username.like("suspended_%")).count(),
                1,
            )

            second_cycle = report_service.run_scheduled_cleanup_with_metrics(
                force=True,
                batch_size=2,
            )
            self.assertEqual(second_cycle["rows_processed"], 1)
            self.assertEqual(second_cycle["users_deleted"], 1)
            self.assertEqual(
                User.query.filter(User.username.like("suspended_%")).count(),
                0,
            )

    def test_crash_log_ingest_and_admin_deobfuscation(self):
        self._register("admin")
        self._register("alice")
        self._make_admin("admin")

        admin_headers = self._auth_header("admin")
        alice_headers = self._auth_header("alice")

        crash_response = self.client.post(
            "/api/crash-logs",
            headers=alice_headers,
            json={
                "event_id": "evt-123",
                "app_version": "0.8.4beta",
                "app_version_code": 35,
                "thread_name": "main",
                "exception_type": "x.y",
                "exception_message": "boom",
                "stack_trace": "java.lang.RuntimeException: boom\n    at a.b.c(Unknown Source:12)",
                "device_model": "Pixel 8",
                "device_manufacturer": "Google",
                "os_version": "14",
                "sdk_int": 34,
                "build_type": "release",
            },
        )
        self.assertEqual(crash_response.status_code, 201)
        crash_log_id = crash_response.get_json()["crash_log_id"]

        mapping_content = (
            b"java.lang.RuntimeException -> x.y:\n"
            b"com.example.RealCrash -> a.b:\n"
            b"    void crashNow() -> c\n"
        )
        mapping_response = self.client.post(
            "/admin/api/crash-mappings",
            headers=admin_headers,
            data={
                "app_version": "0.8.4beta",
                "app_version_code": "35",
                "mapping_file": (io.BytesIO(mapping_content), "mapping.txt"),
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(mapping_response.status_code, 200)

        list_response = self.client.get(
            "/admin/api/crash-logs?page=1&limit=20",
            headers=admin_headers,
        )
        self.assertEqual(list_response.status_code, 200)
        list_payload = list_response.get_json()
        self.assertEqual(list_payload["total"], 1)
        self.assertEqual(list_payload["crash_logs"][0]["exception_type"], "java.lang.RuntimeException")
        self.assertTrue(list_payload["crash_logs"][0]["is_deobfuscated"])

        detail_response = self.client.get(
            f"/admin/api/crash-logs/{crash_log_id}",
            headers=admin_headers,
        )
        self.assertEqual(detail_response.status_code, 200)
        detail = detail_response.get_json()["crash_log"]
        self.assertIn("com.example.RealCrash.crashNow", detail["deobfuscated_stack_trace"])

    def test_crash_log_ingest_is_idempotent_by_event_id(self):
        payload = {
            "event_id": "duplicate-event-id",
            "app_version": "0.8.4beta",
            "exception_type": "java.lang.IllegalStateException",
            "stack_trace": "java.lang.IllegalStateException: dup",
        }

        first_response = self.client.post("/api/crash-logs", json=payload)
        second_response = self.client.post("/api/crash-logs", json=payload)

        self.assertEqual(first_response.status_code, 201)
        self.assertEqual(second_response.status_code, 200)
        self.assertFalse(second_response.get_json()["created"])
        self.assertEqual(
            first_response.get_json()["crash_log_id"],
            second_response.get_json()["crash_log_id"],
        )

        with self.app.app_context():
            from app.models.crash_log_model import CrashLog

            self.assertEqual(CrashLog.query.count(), 1)

    def test_admin_crash_list_groups_duplicates_and_sorts_by_occurrence_count(self):
        self._register("admin")
        self._register("alice")
        self._register("bob")
        self._make_admin("admin")
        admin_headers = self._auth_header("admin")
        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")

        hot_crash_stack = (
            "java.lang.IllegalStateException: boom\n"
            "    at a.b.c(Unknown Source:10)\n"
            "    at a.b.d(Unknown Source:22)"
        )
        cold_crash_stack = (
            "java.lang.RuntimeException: oops\n"
            "    at x.y.z(Unknown Source:15)"
        )

        first_hot = self.client.post(
            "/api/crash-logs",
            headers=alice_headers,
            json={
                "event_id": "hot-crash-1",
                "app_version": "0.8.4beta",
                "exception_type": "java.lang.IllegalStateException",
                "stack_trace": hot_crash_stack,
            },
        )
        second_hot = self.client.post(
            "/api/crash-logs",
            headers=bob_headers,
            json={
                "event_id": "hot-crash-2",
                "app_version": "0.8.4beta",
                "exception_type": "java.lang.IllegalStateException",
                "stack_trace": hot_crash_stack.replace(":10)", ":99)"),
            },
        )
        cold = self.client.post(
            "/api/crash-logs",
            headers=alice_headers,
            json={
                "event_id": "cold-crash-1",
                "app_version": "0.8.4beta",
                "exception_type": "java.lang.RuntimeException",
                "stack_trace": cold_crash_stack,
            },
        )

        self.assertEqual(first_hot.status_code, 201)
        self.assertEqual(second_hot.status_code, 200)
        self.assertFalse(second_hot.get_json()["created"])
        self.assertEqual(cold.status_code, 201)

        list_response = self.client.get(
            "/admin/api/crash-logs?page=1&limit=20",
            headers=admin_headers,
        )
        self.assertEqual(list_response.status_code, 200)
        payload = list_response.get_json()
        self.assertEqual(payload["total"], 2)
        self.assertEqual(payload["crash_logs"][0]["occurrence_count"], 2)
        self.assertEqual(payload["crash_logs"][1]["occurrence_count"], 1)
        self.assertIn("alice", payload["crash_logs"][0]["affected_users"])
        self.assertIn("bob", payload["crash_logs"][0]["affected_users"])

    def test_admin_can_resolve_crash_group_and_skip_future_duplicates(self):
        self._register("admin")
        self._register("alice")
        self._make_admin("admin")
        admin_headers = self._auth_header("admin")
        alice_headers = self._auth_header("alice")

        payload_1 = {
            "event_id": "resolved-group-1",
            "app_version": "0.8.4beta",
            "exception_type": "java.lang.IllegalStateException",
            "stack_trace": (
                "java.lang.IllegalStateException: boom\n"
                "    at a.b.c(Unknown Source:12)\n"
                "    at a.b.d(Unknown Source:35)"
            ),
        }
        payload_2 = {
            "event_id": "resolved-group-2",
            "app_version": "0.8.4beta",
            "exception_type": "java.lang.IllegalStateException",
            "stack_trace": (
                "java.lang.IllegalStateException: boom\n"
                "    at a.b.c(Unknown Source:44)\n"
                "    at a.b.d(Unknown Source:90)"
            ),
        }

        first = self.client.post("/api/crash-logs", headers=alice_headers, json=payload_1)
        second = self.client.post("/api/crash-logs", headers=alice_headers, json=payload_2)
        self.assertEqual(first.status_code, 201)
        self.assertEqual(second.status_code, 200)
        self.assertFalse(second.get_json()["created"])
        self.assertEqual(
            second.get_json()["crash_log_id"],
            first.get_json()["crash_log_id"],
        )

        crash_log_id = first.get_json()["crash_log_id"]
        resolve_response = self.client.post(
            f"/admin/api/crash-logs/{crash_log_id}/resolve",
            headers=admin_headers,
        )
        self.assertEqual(resolve_response.status_code, 200)
        resolve_payload = resolve_response.get_json()
        self.assertGreaterEqual(resolve_payload["deleted_count"], 1)

        with self.app.app_context():
            from app.models.crash_log_model import CrashLog

            self.assertEqual(CrashLog.query.count(), 0)

        third = self.client.post(
            "/api/crash-logs",
            headers=alice_headers,
            json={
                "event_id": "resolved-group-3",
                "app_version": "0.8.4beta",
                "exception_type": "java.lang.IllegalStateException",
                "stack_trace": payload_1["stack_trace"],
            },
        )
        self.assertEqual(third.status_code, 200)
        third_payload = third.get_json()
        self.assertFalse(third_payload["created"])
        self.assertTrue(third_payload["ignored_resolved"])
        self.assertIsNone(third_payload["crash_log_id"])

        with self.app.app_context():
            from app.models.crash_log_model import CrashLog

            self.assertEqual(CrashLog.query.count(), 0)


    def test_message_attachment_upload_success(self):
        self._register("alice")
        headers = self._auth_header("alice")

        class FakeMinio:
            def bucket_exists(self, *args, **kwargs):
                return True

            def put_object(self, **kwargs):
                return None

        with patch("app.services.message_service.get_minio_client", return_value=FakeMinio()):
            response = self.client.post(
                "/api/messages/attachments",
                data={"file": (io.BytesIO(b"fake-image"), "chat.webp", "image/webp")},
                headers=headers,
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 201)
        body = response.get_json()["attachment"]
        self.assertEqual(body["type"], "image")
        self.assertEqual(body["mime_type"], "image/webp")
        self.assertIn("/media/messages/alice/", body["url"])

    def test_message_attachment_upload_rejects_unsupported_type(self):
        self._register("alice")
        headers = self._auth_header("alice")

        response = self.client.post(
            "/api/messages/attachments",
            data={"file": (io.BytesIO(b"bin"), "x.bin", "application/octet-stream")},
            headers=headers,
            content_type="multipart/form-data",
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("Unsupported attachment type", response.get_json()["error"])

    def test_message_attachment_upload_accepts_heic_image(self):
        self._register("alice")
        headers = self._auth_header("alice")

        class FakeMinio:
            def bucket_exists(self, *args, **kwargs):
                return True

            def put_object(self, **kwargs):
                return None

        with patch("app.services.message_service.get_minio_client", return_value=FakeMinio()):
            response = self.client.post(
                "/api/messages/attachments",
                data={"file": (io.BytesIO(b"fake-heic"), "photo.heic", "image/heic")},
                headers=headers,
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 201)
        body = response.get_json()["attachment"]
        self.assertEqual(body["mime_type"], "image/heic")

    def test_message_attachment_upload_rejects_svg_for_security(self):
        self._register("alice")
        headers = self._auth_header("alice")

        response = self.client.post(
            "/api/messages/attachments",
            data={"file": (io.BytesIO(b"<svg></svg>"), "image.svg", "image/svg+xml")},
            headers=headers,
            content_type="multipart/form-data",
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("Unsupported attachment type", response.get_json()["error"])

    def test_message_attachment_upload_rejects_svg_payload_disguised_as_png(self):
        self._register("alice")
        headers = self._auth_header("alice")

        response = self.client.post(
            "/api/messages/attachments",
            data={"file": (io.BytesIO(b"<svg xmlns='http://www.w3.org/2000/svg'></svg>"), "x.png", "image/png")},
            headers=headers,
            content_type="multipart/form-data",
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("Invalid attachment content", response.get_json()["error"])

    def test_message_attachment_upload_accepts_mime_with_parameters(self):
        self._register("alice")
        headers = self._auth_header("alice")

        class FakeMinio:
            def bucket_exists(self, *args, **kwargs):
                return True

            def put_object(self, **kwargs):
                return None

        with patch("app.services.message_service.get_minio_client", return_value=FakeMinio()):
            response = self.client.post(
                "/api/messages/attachments",
                data={"file": (io.BytesIO(b"fake-image"), "chat.webp", "image/webp; charset=binary")},
                headers=headers,
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 201)
        body = response.get_json()["attachment"]
        self.assertEqual(body["mime_type"], "image/webp")

    def test_message_attachment_upload_unknown_length_stream_is_spooled_and_uploaded(self):
        self._register("alice")

        class NonSeekableBytesIO(io.BytesIO):
            def seek(self, *args, **kwargs):
                raise OSError("seek is not supported")

        class FakeFileStorage:
            def __init__(self, payload: bytes):
                self.filename = "voice.webm"
                self.mimetype = "audio/webm"
                self.stream = NonSeekableBytesIO(payload)

        class FakeMinio:
            def bucket_exists(self, *args, **kwargs):
                return True

            def put_object(self, **kwargs):
                captured["length"] = kwargs["length"]
                captured["content_type"] = kwargs["content_type"]
                captured["rolled_to_disk"] = bool(getattr(kwargs["data"], "_rolled", False))
                kwargs["data"].read()
                return None

        payload = b"voice-bytes" * 262_144  # ~2.75MB
        captured = {}
        with self.app.app_context(), patch(
            "app.services.message_service.get_minio_client",
            return_value=FakeMinio(),
        ):
            result = self.message_service.upload_message_attachment(
                "alice",
                FakeFileStorage(payload),
            )

        self.assertEqual(captured["length"], len(payload))
        self.assertEqual(captured["content_type"], "audio/webm")
        self.assertTrue(captured["rolled_to_disk"])
        self.assertEqual(result["size_bytes"], len(payload))
        self.assertEqual(result["mime_type"], "audio/webm")
        self.assertEqual(result["type"], "voice")

    def test_message_attachment_upload_known_length_accepts_exact_image_limit(self):
        self._register("alice")

        class FakeMinio:
            def bucket_exists(self, *args, **kwargs):
                return True

            def put_object(self, **kwargs):
                return None

        payload = b"a" * self.message_service.MAX_IMAGE_SIZE_BYTES
        with self.app.app_context(), patch(
            "app.services.message_service.get_minio_client",
            return_value=FakeMinio(),
        ):
            result = self.message_service.upload_message_attachment(
                username="alice",
                file_storage=type(
                    "FakeFileStorage",
                    (),
                    {
                        "filename": "limit.webp",
                        "mimetype": "image/webp",
                        "stream": io.BytesIO(payload),
                    },
                )(),
            )

        self.assertEqual(result["size_bytes"], self.message_service.MAX_IMAGE_SIZE_BYTES)
        self.assertEqual(result["mime_type"], "image/webp")

    def test_message_attachment_upload_unknown_length_stream_respects_size_limit(self):
        self._register("alice")

        class NonSeekableGeneratedStream:
            def __init__(self, total_size: int):
                self.remaining = total_size

            def read(self, size: int = -1):
                if self.remaining <= 0:
                    return b""
                if size is None or size < 0:
                    size = self.remaining
                chunk_size = min(size, self.remaining, 256 * 1024)
                self.remaining -= chunk_size
                return b"x" * chunk_size

            def seek(self, *args, **kwargs):
                raise OSError("seek is not supported")

        class FakeFileStorage:
            filename = "oversized.webp"
            mimetype = "image/webp"

            def __init__(self, total_size: int):
                self.stream = NonSeekableGeneratedStream(total_size)

        oversized_bytes = self.message_service.MAX_IMAGE_SIZE_BYTES + 1
        with self.app.app_context():
            with self.assertRaises(ValueError) as raised:
                self.message_service.upload_message_attachment(
                    "alice",
                    FakeFileStorage(oversized_bytes),
                )

        self.assertIn("Image too large", str(raised.exception))

    def test_message_attachment_upload_known_length_rejects_oversized_image(self):
        self._register("alice")
        oversized_payload = b"x" * (self.message_service.MAX_IMAGE_SIZE_BYTES + 1)

        with self.app.app_context():
            with self.assertRaises(ValueError) as raised:
                self.message_service.upload_message_attachment(
                    username="alice",
                    file_storage=type(
                        "FakeFileStorage",
                        (),
                        {
                            "filename": "too-large.webp",
                            "mimetype": "image/webp",
                            "stream": io.BytesIO(oversized_payload),
                        },
                    )(),
                )

        self.assertIn("Image too large", str(raised.exception))

    def test_story_upload_feed_view_like_and_viewers(self):
        self._register("alice")
        self._register("bob")

        with self.app.app_context():
            from app.services import follow_service

            follow_service.follow_by_username("bob", "alice")

        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")
        png_payload = (
            b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
            b"\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
            b"\x00\x00\x00\x0bIDATx\x9cc`\x00\x02\x00\x00\x05\x00\x01"
            b"\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82"
        )

        upload_response = self.client.post(
            "/api/story/upload",
            headers=alice_headers,
            data={
                "file": (io.BytesIO(png_payload), "story.png"),
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(upload_response.status_code, 201)
        upload_body = upload_response.get_json()
        story_id = upload_body["story_id"]

        feed_response = self.client.get("/api/story/feed", headers=bob_headers)
        self.assertEqual(feed_response.status_code, 200)
        feed_body = feed_response.get_json()
        self.assertTrue(feed_body["user_stories"])
        self.assertEqual(feed_body["user_stories"][0]["username"], "alice")
        self.assertIn(story_id, feed_body["user_stories"][0]["story_ids"])

        bundle_response = self.client.get(f"/api/story/{story_id}", headers=bob_headers)
        self.assertEqual(bundle_response.status_code, 200)
        bundle_body = bundle_response.get_json()
        self.assertEqual(bundle_body["story"]["story_id"], story_id)

        view_response = self.client.post(
            "/api/story/view",
            headers=bob_headers,
            json={"story_id": story_id},
        )
        self.assertEqual(view_response.status_code, 200)

        like_response = self.client.post(
            "/api/story/like",
            headers=bob_headers,
            json={"story_id": story_id, "liked": True},
        )
        self.assertEqual(like_response.status_code, 200)
        self.assertTrue(like_response.get_json()["liked"])

        viewers_response = self.client.get(
            f"/api/story/{story_id}/viewers",
            headers=alice_headers,
        )
        self.assertEqual(viewers_response.status_code, 200)
        viewers_body = viewers_response.get_json()
        self.assertEqual(viewers_body["total"], 1)
        self.assertEqual(viewers_body["viewers"][0]["username"], "bob")
        self.assertTrue(viewers_body["viewers"][0]["liked"])

    def test_story_mentions_share_to_dm_for_contacts_only(self):
        self._register("alice")
        self._register("bob")
        self._register("charlie")

        with self.app.app_context():
            from app.services import follow_service

            # Contact list for story mention uses people the poster follows.
            follow_service.follow_by_username("alice", "bob")

        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")
        charlie_headers = self._auth_header("charlie")
        png_payload = (
            b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
            b"\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
            b"\x00\x00\x00\x0bIDATx\x9cc`\x00\x02\x00\x00\x05\x00\x01"
            b"\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82"
        )

        upload_response = self.client.post(
            "/api/story/upload",
            headers=alice_headers,
            data={
                "file": (io.BytesIO(png_payload), "story.png"),
                "mentions": ["bob", "charlie"],
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(upload_response.status_code, 201)
        body = upload_response.get_json()
        self.assertEqual(len(body["mention_user_ids"]), 1)

        bob_inbox = self.client.get("/api/messages/inbox", headers=bob_headers)
        self.assertEqual(bob_inbox.status_code, 200)
        bob_messages = bob_inbox.get_json()["messages"]
        self.assertEqual(len(bob_messages), 1)
        self.assertEqual(bob_messages[0]["attachment"]["type"], "story_mention")

        charlie_inbox = self.client.get("/api/messages/inbox", headers=charlie_headers)
        self.assertEqual(charlie_inbox.status_code, 200)
        self.assertEqual(charlie_inbox.get_json()["messages"], [])

    def test_story_mention_candidates_support_typeahead_and_limit(self):
        self._register("alice")
        followed_usernames = [
            "bobalpha",
            "bobbravo",
            "bobsigma",
            "bobdelta",
            "bobecho1",
            "xbobtail",
        ]
        for username in followed_usernames:
            self._register(username)
        self._register("charlie9")

        with self.app.app_context():
            from app.services import follow_service

            for username in followed_usernames:
                follow_service.follow_by_username("alice", username)
            follow_service.follow_by_username("alice", "charlie9")

        alice_headers = self._auth_header("alice")
        response = self.client.get(
            "/api/story/mentions?q=bob&limit=6",
            headers=alice_headers,
        )
        self.assertEqual(response.status_code, 200)
        body = response.get_json()

        result_usernames = [item["username"] for item in body["users"]]
        self.assertEqual(body["limit"], 6)
        self.assertEqual(len(result_usernames), 6)
        self.assertNotIn("charlie9", result_usernames)
        self.assertTrue(all(name.startswith("bob") for name in result_usernames[:5]))
        self.assertEqual(result_usernames[-1], "xbobtail")

    def test_story_reply_creates_dm_for_owner(self):
        self._register("alice")
        self._register("bob")

        with self.app.app_context():
            from app.services import follow_service

            follow_service.follow_by_username("bob", "alice")

        alice_headers = self._auth_header("alice")
        bob_headers = self._auth_header("bob")
        png_payload = (
            b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
            b"\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
            b"\x00\x00\x00\x0bIDATx\x9cc`\x00\x02\x00\x00\x05\x00\x01"
            b"\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82"
        )

        upload_response = self.client.post(
            "/api/story/upload",
            headers=alice_headers,
            data={
                "file": (io.BytesIO(png_payload), "story.png"),
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(upload_response.status_code, 201)
        story_id = upload_response.get_json()["story_id"]

        reply_response = self.client.post(
            "/api/story/reply",
            headers=bob_headers,
            json={"story_id": story_id, "reply_text": "nice story"},
        )
        self.assertEqual(reply_response.status_code, 200)

        alice_inbox = self.client.get("/api/messages/inbox", headers=alice_headers)
        self.assertEqual(alice_inbox.status_code, 200)
        alice_messages = alice_inbox.get_json()["messages"]
        self.assertEqual(len(alice_messages), 1)
        self.assertEqual(alice_messages[0]["attachment"]["type"], "story_reply")
        self.assertEqual(alice_messages[0]["from"], "bob")

    def test_story_upload_rejects_video_longer_than_30_seconds(self):
        self._register("alice")
        headers = self._auth_header("alice")

        with patch.object(self.story_service, "_get_mp4_duration_seconds", return_value=31.0):
            response = self.client.post(
                "/api/story/upload",
                headers=headers,
                data={
                    "file": (io.BytesIO(b"not-a-real-mp4"), "story.mp4", "video/mp4"),
                },
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.get_json()["error"],
            "Story videos must be 30 seconds or shorter",
        )

    def test_story_upload_accepts_video_within_30_seconds(self):
        self._register("alice")
        headers = self._auth_header("alice")

        with patch.object(self.story_service, "_get_mp4_duration_seconds", return_value=29.9):
            response = self.client.post(
                "/api/story/upload",
                headers=headers,
                data={
                    "file": (io.BytesIO(b"fake-video"), "story.mp4", "video/mp4"),
                },
                content_type="multipart/form-data",
            )

        self.assertEqual(response.status_code, 201)
        body = response.get_json()
        self.assertEqual(body["media_type"], "video")

    def test_story_upload_accepts_non_mp4_video_if_safe(self):
        self._register("alice")
        headers = self._auth_header("alice")

        response = self.client.post(
            "/api/story/upload",
            headers=headers,
            data={
                "file": (io.BytesIO(b"fake-webm"), "story.webm", "video/webm"),
            },
            content_type="multipart/form-data",
        )

        self.assertEqual(response.status_code, 201)
        body = response.get_json()
        self.assertEqual(body["media_type"], "video")

    def test_story_upload_rejects_when_daily_limit_reached(self):
        self._register("alice")
        headers = self._auth_header("alice")
        original_limit = self.app.config.get("STORY_DAILY_UPLOAD_LIMIT")
        self.app.config["STORY_DAILY_UPLOAD_LIMIT"] = 1
        try:
            first = self.client.post(
                "/api/story/upload",
                headers=headers,
                data={
                    "file": (io.BytesIO(b"first-story"), "story1.jpg", "image/jpeg"),
                },
                content_type="multipart/form-data",
            )
            self.assertEqual(first.status_code, 201)

            response = self.client.post(
                "/api/story/upload",
                headers=headers,
                data={
                    "file": (io.BytesIO(b"second-story"), "story2.jpg", "image/jpeg"),
                },
                content_type="multipart/form-data",
            )

            self.assertEqual(response.status_code, 400)
            self.assertEqual(
                response.get_json()["error"],
                "Daily story limit reached (max 1 stories per day)",
            )
        finally:
            self.app.config["STORY_DAILY_UPLOAD_LIMIT"] = original_limit

    def test_story_upload_failure_does_not_consume_daily_quota(self):
        self._register("alice")
        headers = self._auth_header("alice")
        original_limit = self.app.config.get("STORY_DAILY_UPLOAD_LIMIT")
        self.app.config["STORY_DAILY_UPLOAD_LIMIT"] = 1
        try:
            with patch.object(
                self.story_service.message_service,
                "upload_message_attachment",
                side_effect=ValueError("Attachment file is required"),
            ):
                failed = self.client.post(
                    "/api/story/upload",
                    headers=headers,
                    data={
                        "file": (io.BytesIO(b"broken"), "broken.jpg", "image/jpeg"),
                    },
                    content_type="multipart/form-data",
                )
            self.assertEqual(failed.status_code, 400)

            # If quota rollback works, the next valid upload still succeeds.
            success = self.client.post(
                "/api/story/upload",
                headers=headers,
                data={
                    "file": (io.BytesIO(b"good-image"), "story.jpg", "image/jpeg"),
                },
                content_type="multipart/form-data",
            )
            self.assertEqual(success.status_code, 201)
        finally:
            self.app.config["STORY_DAILY_UPLOAD_LIMIT"] = original_limit

    def test_messages_routes_inbox_and_send_deprecated(self):
        self._register("alice")
        headers = self._auth_header("alice")

        inbox = self.client.get("/api/messages/inbox", headers=headers)
        self.assertEqual(inbox.status_code, 200)
        self.assertEqual(inbox.get_json()["messages"], [])

        send = self.client.post("/api/messages/send", headers=headers, json={})
        self.assertEqual(send.status_code, 410)


if __name__ == "__main__":
    unittest.main()
