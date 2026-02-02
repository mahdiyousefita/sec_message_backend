from sqlalchemy import func

from app.db import db
from app.models.vote_model import Vote
from app.repositories.comment_repository import create_comment, get_comments_by_post

from app.repositories.comment_repository import (
    get_all_comments_by_post_id,
    get_root_comments_by_post_id,
)


def add_comment(author_id, post_id, text, parent_id=None):
    if not text or not text.strip():
        raise ValueError("Comment text is required")

    comment = create_comment(
        author_id=author_id,
        post_id=post_id,
        text=text,
        parent_id=parent_id
    )

    db.session.commit()
    return comment


# def build_comment_tree(comments):
#     comment_map = {}
#     roots = []
#
#     for c in comments:
#         comment_map[c.id] = {
#             "id": c.id,
#             "author_id": c.author_id,
#             "text": None if c.is_deleted else c.text,
#             "score": get_score("comment", c.id),
#             "created_at": c.created_at.isoformat(),
#             "replies": []
#         }
#
#     for c in comments:
#         node = comment_map[c.id]
#         if c.parent_id:
#             parent = comment_map.get(c.parent_id)
#             if parent:
#                 parent["replies"].append(node)
#         else:
#             roots.append(node)
#
#     return roots
#

def get_post_comments(post_id):
    comments = get_comments_by_post(post_id)
    return build_comment_tree(comments)

def sort_comments(comments: list):
    return sorted(
        comments,
        key=lambda c: (-c.score, -c.created_at.timestamp())
    )

# def build_comment_tree(comments: list):
#     comment_map = {}
#     roots = []
#
#     for comment in comments:
#         comment.replies = []
#         comment_map[comment.id] = comment
#
#     for comment in comments:
#         if comment.parent_id:
#             parent = comment_map.get(comment.parent_id)
#             if parent:
#                 parent.replies.append(comment)
#         else:
#             roots.append(comment)
#
#     def sort_tree(node_list):
#         node_list[:] = sort_comments(node_list)
#         for node in node_list:
#             if node.replies:
#                 sort_tree(node.replies)
#
#     sort_tree(roots)
#     return roots


def build_comment_tree(comments):
    comment_map = {c["id"]: c for c in comments}
    roots = []

    for comment in comments:
        pid = comment["parent_id"]
        if pid:
            parent = comment_map.get(pid)
            if parent:
                parent["replies"].append(comment)
        else:
            roots.append(comment)

    return roots


def get_comments_tree_by_post(
        post_id: int,
        page: int,
        page_size: int
):
    # دریافت همه کامنت‌ها
    raw_comments = get_comments_by_post(post_id)

    # تبدیل ORM به دیکشنری
    all_comments = [serialize_comment(c) for c in raw_comments]

    # ساخت درخت کامنت‌ها
    tree = build_comment_tree(all_comments)

    # دریافت کامنت‌های ریشه‌ای با صفحه‌بندی
    root_comments = get_root_comments_by_post_id(post_id, page, page_size)

    # گرفتن شناسه‌های کامنت‌های ریشه‌ای
    root_ids = {c.id for c in root_comments}

    # فیلتر کردن درخت فقط با ریشه‌های صفحه جاری
    paged_roots = [c for c in tree if c["id"] in root_ids]

    return paged_roots


def serialize_comment(comment):
    return {
        "id": comment.id,
        "text": comment.text,
        "score": comment.score,
        "created_at": comment.created_at,
        "parent_id": comment.parent_id,
        "replies": []
    }
