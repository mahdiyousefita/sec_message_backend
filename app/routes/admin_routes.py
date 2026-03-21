from functools import wraps

from flask import Blueprint, request, jsonify, render_template, make_response
from flask_jwt_extended import (
    create_access_token,
    jwt_required,
    get_jwt_identity,
    verify_jwt_in_request,
)
from werkzeug.security import check_password_hash

from app.db import db
from app.models.user_model import User
from app.models.admin_model import AdminUser
from app.models.post_model import Post
from app.models.comment_model import Comment
from app.models.profile_model import Profile
from app.models.media_model import Media
from app.models.vote_model import Vote
from app.models.follow_model import Follow

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
        })

    return jsonify({"posts": result, "total": total, "page": page, "limit": limit}), 200


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
            "author": author.username if author else f"user-{post.author_id}",
            "created_at": post.created_at.isoformat(),
            "media": [{"id": m.id, "object_name": m.object_name, "mime_type": m.mime_type} for m in media_list],
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
