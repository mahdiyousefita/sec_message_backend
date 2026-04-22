from functools import wraps

from flask import Blueprint, request, jsonify, render_template, make_response
from flask_jwt_extended import (
    create_access_token,
    jwt_required,
    get_jwt_identity,
    verify_jwt_in_request,
)
from werkzeug.security import check_password_hash, generate_password_hash

from app.db import db
from app.models.user_model import User
from app.models.admin_model import AdminUser
from app.models.post_model import Post
from app.models.comment_model import Comment
from app.models.profile_model import Profile
from app.models.media_model import Media
from app.models.vote_model import Vote
from app.models.follow_model import Follow
from app.services import report_service
from app.services import app_update_service
from app.services import crash_log_service
from app.services.post_service import _build_media_url

admin_bp = Blueprint(
    "admin",
    __name__,
    template_folder="../templates",
)


def _is_admin(user_id):
    """Return True when user_id has a row in admin_users."""
    return AdminUser.query.filter_by(user_id=user_id).first() is not None


def admin_required(fn):
    """Decorator: valid JWT + user.is_admin must be True."""
    @wraps(fn)
    def wrapper(*args, **kwargs):
        verify_jwt_in_request()
        username = get_jwt_identity()
        user = User.query.filter_by(username=username).first()
        if not user or not _is_admin(user.id):
            return jsonify({"error": "Forbidden"}), 403
        return fn(*args, **kwargs)
    return wrapper


# ── Pages ────────────────────────────────────────────────────────────

@admin_bp.route("/login")
def admin_login_page():
    return render_template("admin_login.html")


@admin_bp.route("/panel")
def admin_panel_page():
    return render_template("admin_panel.html")


# ── Auth ─────────────────────────────────────────────────────────────

@admin_bp.route("/api/login", methods=["POST"])
def admin_login():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Invalid JSON body"}), 400

    username = (data.get("username") or "").strip()
    password = data.get("password") or ""

    if not username or not password:
        return jsonify({"error": "Invalid credentials"}), 401

    user = User.query.filter_by(username=username).first()
    if not user or not check_password_hash(user.password_hash, password):
        return jsonify({"error": "Invalid credentials"}), 401

    if not _is_admin(user.id):
        return jsonify({"error": "Access denied – not an admin"}), 403

    token = create_access_token(identity=username)
    return jsonify({"access_token": token, "username": username}), 200


@admin_bp.route("/api/me", methods=["GET"])
@admin_required
def admin_me():
    username = get_jwt_identity()
    return jsonify({"username": username}), 200


# ── App update settings ─────────────────────────────────────────────

@admin_bp.route("/api/app-update/settings", methods=["GET"])
@admin_required
def admin_get_app_update_settings():
    platform = request.args.get("platform", "android")
    try:
        settings = app_update_service.get_or_create_config(platform=platform)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"settings": app_update_service.serialize_settings(settings)}), 200


@admin_bp.route("/api/app-update/settings", methods=["PATCH"])
@admin_required
def admin_update_app_update_settings():
    data = request.get_json(silent=True)
    try:
        settings = app_update_service.update_settings(data)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"message": "Settings updated", "settings": settings}), 200


# ── Crash logs + mappings ───────────────────────────────────────────

@admin_bp.route("/api/crash-logs", methods=["GET"])
@admin_required
def admin_list_crash_logs():
    page = max(1, request.args.get("page", 1, type=int))
    limit = min(50, max(1, request.args.get("limit", 20, type=int)))
    app_version = request.args.get("app_version")
    query = request.args.get("q")
    exception_prefix = request.args.get("exception_prefix")

    payload = crash_log_service.list_crash_logs_for_admin(
        page=page,
        limit=limit,
        app_version=app_version,
        query=query,
        exception_prefix=exception_prefix,
    )
    return jsonify(payload), 200


@admin_bp.route("/api/crash-logs/<int:crash_log_id>", methods=["GET"])
@admin_required
def admin_get_crash_log(crash_log_id):
    try:
        payload = crash_log_service.get_crash_log_detail_for_admin(crash_log_id)
    except ValueError as e:
        message = str(e)
        if message == "Crash log not found":
            return jsonify({"error": message}), 404
        return jsonify({"error": message}), 400
    return jsonify({"crash_log": payload}), 200


@admin_bp.route("/api/crash-logs/<int:crash_log_id>/resolve", methods=["POST"])
@admin_required
def admin_resolve_crash_log(crash_log_id):
    admin_username = get_jwt_identity()
    try:
        payload = crash_log_service.resolve_crash_group_for_admin(
            crash_log_id=crash_log_id,
            admin_username=admin_username,
        )
    except ValueError as e:
        message = str(e)
        if message == "Crash log not found":
            return jsonify({"error": message}), 404
        return jsonify({"error": message}), 400

    return jsonify({"message": "Crash group resolved", **payload}), 200


@admin_bp.route("/api/crash-mappings", methods=["GET"])
@admin_required
def admin_list_crash_mappings():
    limit = min(100, max(1, request.args.get("limit", 50, type=int)))
    mappings = crash_log_service.list_mapping_files_for_admin(limit=limit)
    return jsonify({"mappings": mappings}), 200


@admin_bp.route("/api/crash-mappings", methods=["POST"])
@admin_required
def admin_upload_crash_mapping():
    uploaded_file = request.files.get("mapping_file")
    if uploaded_file is None:
        return jsonify({"error": "mapping_file is required"}), 400

    admin_username = get_jwt_identity()
    app_version = request.form.get("app_version")
    app_version_code = request.form.get("app_version_code")

    try:
        mapping_payload = crash_log_service.upload_mapping_file_for_admin(
            app_version=app_version,
            mapping_bytes=uploaded_file.read(),
            original_filename=uploaded_file.filename,
            admin_username=admin_username,
            app_version_code=app_version_code,
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    return jsonify({"message": "Mapping file uploaded", "mapping": mapping_payload}), 200


# ── Search ───────────────────────────────────────────────────────────

@admin_bp.route("/api/users", methods=["GET"])
@admin_required
def admin_search_users():
    q = request.args.get("q", "").strip()
    page = max(1, request.args.get("page", 1, type=int))
    limit = min(50, max(1, request.args.get("limit", 20, type=int)))

    query = User.query
    if q:
        pattern = f"%{q}%"
        query = query.filter(User.username.ilike(pattern))
    query = query.order_by(User.id.asc())

    total = query.count()
    users = query.offset((page - 1) * limit).limit(limit).all()

    profiles = {}
    if users:
        user_ids = [u.id for u in users]
        for p in Profile.query.filter(Profile.user_id.in_(user_ids)).all():
            profiles[p.user_id] = p

    admin_ids = set()
    if users:
        user_ids = [u.id for u in users]
        admin_ids = {a.user_id for a in AdminUser.query.filter(AdminUser.user_id.in_(user_ids)).all()}

    result = []
    for u in users:
        prof = profiles.get(u.id)
        result.append({
            "id": u.id,
            "username": u.username,
            "is_admin": u.id in admin_ids,
            "name": prof.name if prof else u.username,
        })

    return jsonify({"users": result, "total": total, "page": page, "limit": limit}), 200


@admin_bp.route("/api/online-users", methods=["GET"])
@admin_required
def admin_list_online_users():
    from app.socket_events import get_online_usernames

    online_usernames = get_online_usernames()
    users_payload = _build_admin_user_payload(online_usernames)
    return jsonify({"users": users_payload, "total": len(users_payload)}), 200


@admin_bp.route("/api/recently-online-users", methods=["GET"])
@admin_required
def admin_list_recently_online_users():
    from app.socket_events import get_recently_online_usernames

    recent_usernames = get_recently_online_usernames(window_hours=24)
    users_payload = _build_admin_user_payload(recent_usernames)
    return jsonify({"users": users_payload, "total": len(users_payload)}), 200


def _build_admin_user_payload(usernames):
    if not usernames:
        return []

    users = (
        User.query
        .filter(User.username.in_(usernames))
        .order_by(User.username.asc())
        .all()
    )

    user_ids = [user.id for user in users]
    profiles = {}
    if user_ids:
        for profile in Profile.query.filter(Profile.user_id.in_(user_ids)).all():
            profiles[profile.user_id] = profile

    admin_ids = set()
    if user_ids:
        admin_ids = {
            admin.user_id
            for admin in AdminUser.query.filter(AdminUser.user_id.in_(user_ids)).all()
        }

    payload = []
    for user in users:
        profile = profiles.get(user.id)
        payload.append(
            {
                "id": user.id,
                "username": user.username,
                "name": profile.name if profile else user.username,
                "is_admin": user.id in admin_ids,
            }
        )

    return payload


@admin_bp.route("/api/posts", methods=["GET"])
@admin_required
def admin_search_posts():
    q = request.args.get("q", "").strip()
    page = max(1, request.args.get("page", 1, type=int))
    limit = min(50, max(1, request.args.get("limit", 20, type=int)))

    query = Post.query
    if q:
        pattern = f"%{q}%"
        query = query.filter(Post.text.ilike(pattern))
    query = query.order_by(Post.created_at.desc())

    total = query.count()
    posts = query.offset((page - 1) * limit).limit(limit).all()

    author_ids = {p.author_id for p in posts}
    user_by_id = {u.id: u for u in User.query.filter(User.id.in_(author_ids)).all()} if author_ids else {}

    result = []
    for p in posts:
        author = user_by_id.get(p.author_id)
        result.append({
            "id": p.id,
            "text": p.text,
            "author": author.username if author else f"user-{p.author_id}",
            "created_at": p.created_at.isoformat(),
            "followers_only": bool(getattr(p, "followers_only", False)),
            "is_hidden": bool(getattr(p, "is_hidden", False)),
        })

    return jsonify({"posts": result, "total": total, "page": page, "limit": limit}), 200


# ── Reports ──────────────────────────────────────────────────────────

@admin_bp.route("/api/reports", methods=["GET"])
@admin_required
def admin_list_reports():
    page = max(1, request.args.get("page", 1, type=int))
    limit = min(50, max(1, request.args.get("limit", 20, type=int)))
    status = request.args.get("status")
    report_type = request.args.get("report_type")

    try:
        data = report_service.list_reports_for_admin(
            page=page,
            limit=limit,
            status=status,
            report_type=report_type,
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    return jsonify(data), 200


@admin_bp.route("/api/reports/<int:report_id>", methods=["GET"])
@admin_required
def admin_get_report(report_id):
    try:
        payload = report_service.get_report_detail_for_admin(report_id)
    except ValueError as e:
        if str(e) == "Report not found":
            return jsonify({"error": str(e)}), 404
        return jsonify({"error": str(e)}), 400

    return jsonify({"report": payload}), 200


@admin_bp.route("/api/reports/<int:report_id>/handle", methods=["POST"])
@admin_required
def admin_handle_report(report_id):
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Invalid JSON body"}), 400

    admin_username = get_jwt_identity()

    try:
        report_service.handle_report_by_admin(
            report_id=report_id,
            admin_username=admin_username,
            decision=data.get("decision"),
            admin_note=data.get("admin_note"),
        )
        report_payload = report_service.get_report_detail_for_admin(report_id)
    except ValueError as e:
        message = str(e)
        if message in {"Report not found", "Post not found", "User not found"}:
            return jsonify({"error": message}), 404
        return jsonify({"error": message}), 400

    return jsonify(
        {
            "message": "Report handled",
            "report": report_payload,
        }
    ), 200


# ── Post detail + comments ──────────────────────────────────────────

@admin_bp.route("/api/posts/<int:post_id>", methods=["GET"])
@admin_required
def admin_get_post(post_id):
    post = Post.query.get(post_id)
    if not post:
        return jsonify({"error": "Post not found"}), 404

    author = User.query.get(post.author_id)
    media_list = Media.query.filter_by(post_id=post.id).all()

    comments = (
        Comment.query
        .filter_by(post_id=post.id)
        .order_by(Comment.created_at.asc())
        .all()
    )
    comment_author_ids = {c.author_id for c in comments}
    comment_users = {u.id: u for u in User.query.filter(User.id.in_(comment_author_ids)).all()} if comment_author_ids else {}

    return jsonify({
        "post": {
            "id": post.id,
            "text": post.text,
            "quoted_post_id": post.quoted_post_id,
            "author": author.username if author else f"user-{post.author_id}",
            "created_at": post.created_at.isoformat(),
            "followers_only": bool(getattr(post, "followers_only", False)),
            "is_hidden": bool(getattr(post, "is_hidden", False)),
            "media": [
                {
                    "id": m.id,
                    "object_name": m.object_name,
                    "mime_type": m.mime_type,
                    "url": _build_media_url(m.object_name),
                }
                for m in media_list
            ],
        },
        "comments": [
            {
                "id": c.id,
                "text": c.text,
                "author": comment_users[c.author_id].username if c.author_id in comment_users else f"user-{c.author_id}",
                "parent_id": c.parent_id,
                "created_at": c.created_at.isoformat(),
            }
            for c in comments
        ],
    }), 200


# ── Delete post ──────────────────────────────────────────────────────

@admin_bp.route("/api/posts/<int:post_id>", methods=["DELETE"])
@admin_required
def admin_delete_post(post_id):
    post = Post.query.get(post_id)
    if not post:
        return jsonify({"error": "Post not found"}), 404

    Comment.query.filter_by(post_id=post.id).delete()
    Vote.query.filter(
        (Vote.target_type == "post") & (Vote.target_id == post.id)
    ).delete()
    Post.query.filter(
        Post.quoted_post_id == post.id
    ).update(
        {Post.quoted_post_id: None},
        synchronize_session=False,
    )
    Media.query.filter_by(post_id=post.id).delete()
    db.session.delete(post)
    db.session.commit()
    return jsonify({"message": "Post deleted"}), 200


# ── Delete comment ───────────────────────────────────────────────────

@admin_bp.route("/api/comments/<int:comment_id>", methods=["DELETE"])
@admin_required
def admin_delete_comment(comment_id):
    comment = Comment.query.get(comment_id)
    if not comment:
        return jsonify({"error": "Comment not found"}), 404

    _delete_comment_tree(comment.id)
    db.session.commit()
    return jsonify({"message": "Comment deleted"}), 200


def _delete_comment_tree(comment_id):
    """Recursively delete a comment and all its child replies."""
    children = Comment.query.filter_by(parent_id=comment_id).all()
    for child in children:
        _delete_comment_tree(child.id)
    Vote.query.filter(
        (Vote.target_type == "comment") & (Vote.target_id == comment_id)
    ).delete()
    Comment.query.filter_by(id=comment_id).delete()


# ── Delete user ──────────────────────────────────────────────────────

@admin_bp.route("/api/users/<int:user_id>", methods=["DELETE"])
@admin_required
def admin_delete_user(user_id):
    user = User.query.get(user_id)
    if not user:
        return jsonify({"error": "User not found"}), 404

    current_admin = get_jwt_identity()
    if user.username == current_admin:
        return jsonify({"error": "Cannot delete yourself"}), 400

    user_posts = Post.query.filter_by(author_id=user.id).all()
    for post in user_posts:
        Comment.query.filter_by(post_id=post.id).delete()
        Vote.query.filter(
            (Vote.target_type == "post") & (Vote.target_id == post.id)
        ).delete()
        Post.query.filter(
            Post.quoted_post_id == post.id
        ).update(
            {Post.quoted_post_id: None},
            synchronize_session=False,
        )
        Media.query.filter_by(post_id=post.id).delete()
        db.session.delete(post)

    Comment.query.filter_by(author_id=user.id).delete()
    Vote.query.filter_by(user_id=user.id).delete()
    Follow.query.filter(
        (Follow.follower_id == user.id) | (Follow.following_id == user.id)
    ).delete()
    Profile.query.filter_by(user_id=user.id).delete()
    db.session.delete(user)
    db.session.commit()
    return jsonify({"message": "User deleted"}), 200


# ── Promote / demote admin ──────────────────────────────────────────

@admin_bp.route("/api/users/<int:user_id>/promote", methods=["POST"])
@admin_required
def admin_promote_user(user_id):
    user = User.query.get(user_id)
    if not user:
        return jsonify({"error": "User not found"}), 404

    if _is_admin(user.id):
        return jsonify({"message": f"{user.username} is already an admin"}), 200

    db.session.add(AdminUser(user_id=user.id))
    db.session.commit()
    return jsonify({"message": f"{user.username} is now an admin"}), 200


@admin_bp.route("/api/users/<int:user_id>/demote", methods=["POST"])
@admin_required
def admin_demote_user(user_id):
    user = User.query.get(user_id)
    if not user:
        return jsonify({"error": "User not found"}), 404

    current_admin = get_jwt_identity()
    if user.username == current_admin:
        return jsonify({"error": "Cannot demote yourself"}), 400

    AdminUser.query.filter_by(user_id=user.id).delete()
    db.session.commit()
    return jsonify({"message": f"{user.username} is no longer an admin"}), 200


# ── Update user credentials ─────────────────────────────────────────

@admin_bp.route("/api/users/<int:user_id>/credentials", methods=["PATCH"])
@admin_required
def admin_update_user_credentials(user_id):
    user = User.query.get(user_id)
    if not user:
        return jsonify({"error": "User not found"}), 404

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Invalid JSON body"}), 400

    incoming_username = data.get("username")
    incoming_password = data.get("password")

    if incoming_username is None and incoming_password is None:
        return jsonify({"error": "At least one field (username or password) is required"}), 400

    changed_fields = []

    if incoming_username is not None:
        if not isinstance(incoming_username, str) or not incoming_username.strip():
            return jsonify({"error": "Username must be a non-empty string"}), 400

        new_username = incoming_username.strip()
        existing = User.query.filter_by(username=new_username).first()
        if existing and existing.id != user.id:
            return jsonify({"error": "Username already exists"}), 409

        if user.username != new_username:
            user.username = new_username
            changed_fields.append("username")

    if incoming_password is not None:
        if not isinstance(incoming_password, str) or not incoming_password.strip():
            return jsonify({"error": "Password must be a non-empty string"}), 400

        user.password_hash = generate_password_hash(incoming_password)
        changed_fields.append("password")

    if not changed_fields:
        return jsonify(
            {
                "message": "No changes applied",
                "user": {
                    "id": user.id,
                    "username": user.username,
                },
                "changed_fields": [],
            }
        ), 200

    db.session.commit()

    return jsonify(
        {
            "message": "User credentials updated",
            "user": {
                "id": user.id,
                "username": user.username,
            },
            "changed_fields": changed_fields,
        }
    ), 200
