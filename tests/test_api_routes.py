import os
import io
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
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


class TestApiRoutes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db_fd, cls.db_path = tempfile.mkstemp(suffix=".db")
        os.close(db_fd)
        os.environ["DATABASE_URL"] = f"sqlite:///{cls.db_path}"
        os.environ["JWT_SECRET_KEY"] = "test-secret"

        from app import create_app
        from app.db import db
        from app.services import auth_service
        from app.models.comment_model import Comment
        from app.repositories import message_repository
        from app.extensions import redis_client as redis_module
        import app.routes.contact_routes as contact_routes
        import app.socket_events as socket_events

        cls.app = create_app()
        cls.client = cls.app.test_client()
        cls.db = db
        cls.auth_service = auth_service
        cls.Comment = Comment
        cls.socket_events = socket_events

        cls.fake_redis = FakeRedis()
        cls.redis_patches = [
            patch.object(message_repository, "redis_client", cls.fake_redis),
            patch.object(redis_module, "redis_client", cls.fake_redis),
            patch.object(contact_routes, "r", cls.fake_redis),
        ]
        for patcher in cls.redis_patches:
            patcher.start()

    @classmethod
    def tearDownClass(cls):
        for patcher in cls.redis_patches:
            patcher.stop()

        cls.socket_events._online_users.clear()
        cls.socket_events._registered = False

        if os.path.exists(cls.db_path):
            os.remove(cls.db_path)

    def setUp(self):
        with self.app.app_context():
            self.db.drop_all()
            self.db.create_all()
        self.fake_redis.clear()
        uploads_dir = os.path.join(self.app.static_folder, "uploads")
        if os.path.isdir(uploads_dir):
            shutil.rmtree(uploads_dir)

    def _register(self, username, password="pass123", public_key=None):
        public_key = public_key or f"{username}_pub_key"
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
