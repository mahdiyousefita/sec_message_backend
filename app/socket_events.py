from flask import current_app, has_request_context, request, session
import logging
import threading
from datetime import datetime, timedelta, timezone
from uuid import uuid4
from flask_jwt_extended import decode_token
from flask_socketio import emit, join_room

from app.extensions.extensions import socketio
from app.extensions import redis_client as redis_module
from app.services import message_service
from app.services import activity_notification_service
from app.services import async_task_service
from app.services import group_notification_service
from app.services import notification_service
from app.services.group_delivery_guard import GroupDeliveryGuard

logger = logging.getLogger(__name__)

_registered = False
_user_sids = {}
_sid_group_rooms = {}
_presence_state_lock = threading.Lock()
_presence_maintenance_started = False

PRESENCE_ONLINE_USERS_KEY = "presence:online_users"
PRESENCE_RECENTLY_ONLINE_KEY = "presence:recently_online"
PRESENCE_CONNECTIONS_PREFIX = "presence:connections:"
PRESENCE_CONNECTION_TOKEN_PREFIX = "presence:connection_token:"
_PROCESS_INSTANCE_ID = uuid4().hex
_presence_connection_ttl_seconds = 75
_presence_heartbeat_interval_seconds = 20
_presence_cleanup_batch_size = 200


def _extract_access_token(auth):
    if isinstance(auth, dict):
        token = auth.get("token") or auth.get("access_token")
        if token:
            return token

    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        return auth_header.split(" ", 1)[1].strip()

    return request.args.get("token")


def _presence_client():
    return redis_module.redis_client


def _normalize_username(username):
    if not isinstance(username, str):
        return ""
    return username.strip()


def _decode_redis_value(value):
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except Exception:
            return ""
    if isinstance(value, str):
        return value
    return ""


def _presence_connections_key(username):
    return f"{PRESENCE_CONNECTIONS_PREFIX}{username}"


def _presence_connection_token_key(token):
    return f"{PRESENCE_CONNECTION_TOKEN_PREFIX}{token}"


def _presence_connection_token(sid):
    if not isinstance(sid, str) or not sid.strip():
        return ""
    return f"{_PROCESS_INSTANCE_ID}:{sid.strip()}"


def _build_profile_image_url(image_object_name):
    if not isinstance(image_object_name, str) or not image_object_name.strip():
        return None
    normalized_name = image_object_name.strip()
    base_url = current_app.config.get("APP_PUBLIC_BASE_URL", "").rstrip("/")
    if not base_url and has_request_context():
        base_url = request.url_root.rstrip("/")
    if normalized_name.startswith("static/"):
        return f"{base_url}/{normalized_name}" if base_url else f"/{normalized_name}"
    return f"{base_url}/media/{normalized_name}" if base_url else f"/media/{normalized_name}"


def _resolve_positive_int(value, default_value, minimum):
    try:
        parsed_value = int(value)
    except (TypeError, ValueError):
        parsed_value = default_value
    return max(minimum, parsed_value)


def _cleanup_user_connection_tokens(username):
    normalized_username = _normalize_username(username)
    if not normalized_username:
        return 0

    try:
        client = _presence_client()
        connections_key = _presence_connections_key(normalized_username)
        raw_tokens = client.smembers(connections_key) or set()
        stale_tokens = []

        for raw_token in raw_tokens:
            token = _decode_redis_value(raw_token).strip()
            if not token:
                stale_tokens.append(raw_token)
                continue

            token_owner = _decode_redis_value(
                client.get(_presence_connection_token_key(token))
            ).strip()
            if token_owner != normalized_username:
                stale_tokens.append(token)

        if stale_tokens:
            client.srem(connections_key, *stale_tokens)

        active_connections = int(client.scard(connections_key) or 0)
        if active_connections > 0:
            client.sadd(PRESENCE_ONLINE_USERS_KEY, normalized_username)
            return active_connections

        client.delete(connections_key)
        client.srem(PRESENCE_ONLINE_USERS_KEY, normalized_username)
        return 0
    except Exception as exc:
        logger.warning(
            "Failed to cleanup presence tokens for username=%s: %s",
            normalized_username,
            exc,
        )
        return 0


def _refresh_presence_connection(username, sid, touch_recently_online=True):
    normalized_username = _normalize_username(username)
    connection_token = _presence_connection_token(sid)
    if not normalized_username or not connection_token:
        return False

    try:
        client = _presence_client()
        client.sadd(_presence_connections_key(normalized_username), connection_token)
        client.setex(
            _presence_connection_token_key(connection_token),
            _presence_connection_ttl_seconds,
            normalized_username,
        )
        client.sadd(PRESENCE_ONLINE_USERS_KEY, normalized_username)
        if touch_recently_online:
            _touch_recently_online(normalized_username)
        return True
    except Exception as exc:
        logger.warning(
            "Failed to refresh presence token for username=%s sid=%s: %s",
            normalized_username,
            sid,
            exc,
        )
        return False


def _set_user_online(username, sid):
    normalized_username = _normalize_username(username)
    if not normalized_username:
        return False

    active_before = _cleanup_user_connection_tokens(normalized_username)
    refreshed = _refresh_presence_connection(normalized_username, sid)
    if not refreshed:
        return False
    active_after = _cleanup_user_connection_tokens(normalized_username)
    return active_before <= 0 < active_after


def _set_user_offline(username, sid):
    normalized_username = _normalize_username(username)
    connection_token = _presence_connection_token(sid)
    if not normalized_username:
        return False

    active_before = _cleanup_user_connection_tokens(normalized_username)
    try:
        client = _presence_client()
        if connection_token:
            client.srem(_presence_connections_key(normalized_username), connection_token)
            client.delete(_presence_connection_token_key(connection_token))
        _touch_recently_online(normalized_username)
        active_after = _cleanup_user_connection_tokens(normalized_username)
        return active_before > 0 and active_after <= 0
    except Exception as exc:
        logger.warning(
            "Failed to mark user offline for presence tracking username=%s: %s",
            normalized_username,
            exc,
        )
        return False


def _register_user_sid(username, sid):
    if not username or not sid:
        return
    with _presence_state_lock:
        sid_set = _user_sids.setdefault(username, set())
        sid_set.add(sid)


def _unregister_user_sid(username, sid):
    if not sid:
        return

    if username:
        with _presence_state_lock:
            sid_set = _user_sids.get(username)
            if not sid_set:
                return
            sid_set.discard(sid)
            if not sid_set:
                _user_sids.pop(username, None)
        return

    with _presence_state_lock:
        stale_usernames = []
        for known_username, sid_set in _user_sids.items():
            sid_set.discard(sid)
            if not sid_set:
                stale_usernames.append(known_username)
        for stale_username in stale_usernames:
            _user_sids.pop(stale_username, None)


def _track_group_room_join(sid, group_id):
    if not sid:
        return
    try:
        normalized_group_id = int(group_id)
    except (TypeError, ValueError):
        return
    if normalized_group_id <= 0:
        return

    with _presence_state_lock:
        memberships = _sid_group_rooms.setdefault(sid, set())
        memberships.add(normalized_group_id)


def _track_group_room_leave(sid, group_id):
    if not sid:
        return
    try:
        normalized_group_id = int(group_id)
    except (TypeError, ValueError):
        return
    if normalized_group_id <= 0:
        return

    with _presence_state_lock:
        memberships = _sid_group_rooms.get(sid)
        if not memberships:
            return
        memberships.discard(normalized_group_id)
        if not memberships:
            _sid_group_rooms.pop(sid, None)


def _authorized_group_sids(username, group_id):
    normalized_username = _normalize_username(username)
    if not normalized_username:
        return []

    try:
        normalized_group_id = int(group_id)
    except (TypeError, ValueError):
        return []
    if normalized_group_id <= 0:
        return []

    with _presence_state_lock:
        sids = list(_user_sids.get(normalized_username, set()))
        sid_group_rooms_snapshot = {
            sid: set(_sid_group_rooms.get(sid, set()))
            for sid in sids
        }
    return [
        sid
        for sid in sids
        if normalized_group_id in sid_group_rooms_snapshot.get(sid, set())
    ]


def emit_group_event_to_members(group_id, event_name, payload, exclude_usernames=None):
    try:
        normalized_group_id = int(group_id)
    except (TypeError, ValueError):
        return 0

    excluded = {
        username.strip()
        for username in (exclude_usernames or [])
        if isinstance(username, str) and username.strip()
    }

    from app.repositories import group_repository

    dispatched = 0
    member_usernames = list(dict.fromkeys(
        group_repository.get_group_member_usernames(normalized_group_id)
    ))
    for member_username in member_usernames:
        if member_username in excluded:
            continue
        socketio.emit(event_name, payload, room=member_username)
        dispatched += 1
    return dispatched


def evict_user_from_group_room(username, group_id, *, reason="membership_removed", notify=True):
    if not isinstance(username, str) or not username.strip():
        return 0

    try:
        normalized_group_id = int(group_id)
    except (TypeError, ValueError):
        return 0

    normalized_username = username.strip()
    room_name = f"group_{normalized_group_id}"
    with _presence_state_lock:
        sids = list(_user_sids.get(normalized_username, set()))
    evicted = 0
    for sid in sids:
        try:
            socketio.server.leave_room(sid, room_name, namespace="/")
            _track_group_room_leave(sid, normalized_group_id)
            evicted += 1
        except Exception as exc:
            logger.warning(
                "Failed to evict sid %s for user %s from room %s: %s",
                sid, normalized_username, room_name, exc,
            )

    if notify:
        socketio.emit(
            "group_membership_revoked",
            {
                "group_id": normalized_group_id,
                "reason": reason,
            },
            room=normalized_username,
        )

    return evicted


def is_user_online(username):
    normalized_username = _normalize_username(username)
    if not normalized_username:
        return False

    return _cleanup_user_connection_tokens(normalized_username) > 0


def get_users_online_status(usernames):
    if not isinstance(usernames, (list, tuple, set)):
        return {}

    normalized = []
    seen = set()
    for raw_username in usernames:
        username = _normalize_username(raw_username)
        if not username or username in seen:
            continue
        seen.add(username)
        normalized.append(username)

    if not normalized:
        return {}

    status_by_username = {username: False for username in normalized}

    try:
        client = _presence_client()
        pipe = client.pipeline()
        for username in normalized:
            pipe.sismember(PRESENCE_ONLINE_USERS_KEY, username)
        memberships = pipe.execute()
    except Exception as exc:
        logger.warning("Failed to batch fetch online presence: %s", exc)
        return {
            username: is_user_online(username)
            for username in normalized
        }

    for username, is_member in zip(normalized, memberships):
        if not is_member:
            continue
        status_by_username[username] = _cleanup_user_connection_tokens(username) > 0

    return status_by_username


def get_online_usernames():
    try:
        client = _presence_client()
        usernames = set()
        raw_usernames = list(client.smembers(PRESENCE_ONLINE_USERS_KEY) or set())
        stale_usernames = []
        for raw_username in raw_usernames:
            username = _decode_redis_value(raw_username).strip()
            if not username:
                continue
            if _cleanup_user_connection_tokens(username) > 0:
                usernames.add(username)
            else:
                stale_usernames.append(username)

        if stale_usernames:
            client.srem(PRESENCE_ONLINE_USERS_KEY, *stale_usernames)

        return sorted(usernames)
    except Exception as exc:
        logger.warning("Failed to fetch online usernames from presence store: %s", exc)
        return []


def get_group_online_users_payload(group_id):
    try:
        normalized_group_id = int(group_id)
    except (TypeError, ValueError):
        return None

    if normalized_group_id <= 0:
        return None

    try:
        from app.repositories import group_repository

        members = group_repository.get_group_members(normalized_group_id)
        usernames = [
            (member.get("username") or "").strip()
            for member in members
            if isinstance(member, dict)
        ]
        online_status_by_username = get_users_online_status(usernames)
        online_users = []
        for member in members:
            if not isinstance(member, dict):
                continue
            username = (member.get("username") or "").strip()
            if not username or not online_status_by_username.get(username, False):
                continue

            online_users.append(
                {
                    "user_id": str(member.get("id") or ""),
                    "username": username,
                    "badge": member.get("badge"),
                    "profile_image_url": _build_profile_image_url(
                        member.get("image_object_name")
                    ),
                    "profile_image_shape": member.get("profile_image_shape", "circle"),
                }
            )

        return {
            "group_id": normalized_group_id,
            "online_users": online_users,
        }
    except Exception as exc:
        logger.warning(
            "Failed to build group online payload for group_id=%s: %s",
            group_id,
            exc,
        )
        return None


def _emit_group_presence_changed(username, online):
    normalized_username = _normalize_username(username)
    if not normalized_username:
        return

    try:
        from app.repositories import group_repository, user_repository

        user = user_repository.get_by_username(normalized_username)
        if not user:
            return
        user_payload = _build_group_user_payload(normalized_username)
        if not user_payload:
            return

        for group in group_repository.get_groups_for_user(user.id):
            socketio.emit(
                "group_presence_changed",
                {
                    "group_id": int(group.id),
                    "online": bool(online),
                    "user": user_payload,
                },
                room=f"group_{group.id}",
            )
    except Exception as exc:
        logger.warning(
            "Failed to emit group presence update username=%s online=%s: %s",
            normalized_username,
            online,
            exc,
        )


def _build_group_user_payload(username):
    normalized_username = _normalize_username(username)
    if not normalized_username:
        return None

    try:
        from app.repositories import profile_repository, user_repository

        user = user_repository.get_by_username(normalized_username)
        if not user:
            return None
        profile = profile_repository.get_by_user_id(user.id)
        return {
            "user_id": str(user.id),
            "username": normalized_username,
            "badge": user.badge,
            "profile_image_url": _build_profile_image_url(
                profile.image_object_name if profile else None
            ),
            "profile_image_shape": (
                profile.profile_image_shape if profile and profile.profile_image_shape else "circle"
            ),
        }
    except Exception as exc:
        logger.warning(
            "Failed to build group user payload for username=%s: %s",
            normalized_username,
            exc,
        )
        return {
            "user_id": "",
            "username": normalized_username,
            "badge": None,
            "profile_image_url": None,
            "profile_image_shape": "circle",
        }


def _normalize_utc(timestamp):
    if not isinstance(timestamp, datetime):
        return None
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def _resolve_client_message_id(raw_value, *, required=False):
    if raw_value is None:
        if required:
            return None, "client_message_id is required"
        return None, None
    if not isinstance(raw_value, str):
        return None, "client_message_id must be a string"
    normalized = raw_value.strip()
    if not normalized:
        return None, "client_message_id cannot be blank"
    if len(normalized) > 128:
        return None, "client_message_id is too long"
    return normalized, None


def _build_contacts_update_payload(
    *,
    contact,
    from_username,
    to_username,
    timestamp,
    message_type,
    has_unread,
    unread_count=0,
    sync_reason="message",
):
    payload = {
        "contact": contact,
        "from": from_username,
        "to": to_username,
        "timestamp": timestamp,
        "type": message_type,
        "has_unread": bool(has_unread),
        "unread_count": max(0, int(unread_count or 0)),
        "sync_reason": sync_reason,
    }
    return payload


def _touch_recently_online(username, seen_at=None):
    normalized_username = _normalize_username(username)
    if not normalized_username:
        return

    resolved_seen_at = _normalize_utc(seen_at) or datetime.now(timezone.utc)
    score = resolved_seen_at.timestamp()
    try:
        _presence_client().zadd(PRESENCE_RECENTLY_ONLINE_KEY, {normalized_username: score})
    except Exception as exc:
        logger.warning(
            "Failed to update recently-online presence for %s: %s",
            normalized_username,
            exc,
        )


def get_recently_online_usernames(window_hours=24):
    try:
        resolved_window_hours = max(1, int(window_hours))
    except (TypeError, ValueError):
        resolved_window_hours = 24

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=resolved_window_hours)

    # Keep users with active sockets in the rolling window even during long sessions.
    online_usernames = get_online_usernames()
    for username in online_usernames:
        _touch_recently_online(username, seen_at=now)

    cutoff_ts = cutoff.timestamp()
    try:
        client = _presence_client()
        stale_entries = []
        for member, score in client.zrange(
            PRESENCE_RECENTLY_ONLINE_KEY, 0, -1, withscores=True
        ):
            member_name = _decode_redis_value(member).strip()
            member_score = float(score)
            if member_score < cutoff_ts:
                if member_name:
                    stale_entries.append(member_name)
                continue
            break
        if stale_entries:
            client.zrem(PRESENCE_RECENTLY_ONLINE_KEY, *stale_entries)

        active_usernames = []
        for member, score in client.zrevrange(
            PRESENCE_RECENTLY_ONLINE_KEY, 0, -1, withscores=True
        ):
            member_name = _decode_redis_value(member).strip()
            if not member_name:
                continue
            if float(score) < cutoff_ts:
                continue
            active_usernames.append(member_name)
        return sorted(set(active_usernames))
    except Exception as exc:
        logger.warning("Failed to fetch recently-online usernames from presence store: %s", exc)
        return sorted(online_usernames)


def _snapshot_local_connections():
    with _presence_state_lock:
        return [
            (username, list(sids))
            for username, sids in _user_sids.items()
            if username and sids
        ]


def _refresh_local_presence_connections():
    connection_snapshot = _snapshot_local_connections()
    touched_usernames = set()
    for username, sid_list in connection_snapshot:
        if not sid_list:
            continue
        touched_usernames.add(username)
        for sid in sid_list:
            _refresh_presence_connection(
                username=username,
                sid=sid,
                touch_recently_online=False,
            )
        _cleanup_user_connection_tokens(username)

    for username in touched_usernames:
        _touch_recently_online(username)


def _cleanup_online_presence_sample():
    try:
        client = _presence_client()
        raw_usernames = list(client.smembers(PRESENCE_ONLINE_USERS_KEY) or set())
    except Exception as exc:
        logger.warning("Failed to load online presence set for cleanup: %s", exc)
        return

    stale_usernames = []
    for raw_username in raw_usernames[:_presence_cleanup_batch_size]:
        username = _decode_redis_value(raw_username).strip()
        if not username:
            continue
        if _cleanup_user_connection_tokens(username) <= 0:
            stale_usernames.append(username)

    if stale_usernames:
        try:
            client.srem(PRESENCE_ONLINE_USERS_KEY, *stale_usernames)
        except Exception as exc:
            logger.warning("Failed to prune stale usernames from online presence set: %s", exc)


def _presence_maintenance_loop():
    while True:
        try:
            socketio.sleep(_presence_heartbeat_interval_seconds)
            _refresh_local_presence_connections()
            _cleanup_online_presence_sample()
        except Exception as exc:
            logger.warning("Presence maintenance loop iteration failed: %s", exc)
            socketio.sleep(1)


def _start_presence_maintenance_loop():
    global _presence_maintenance_started
    if _presence_maintenance_started:
        return
    _presence_maintenance_started = True
    socketio.start_background_task(_presence_maintenance_loop)


def register_socket_events():
    global _registered
    global _presence_connection_ttl_seconds
    global _presence_heartbeat_interval_seconds
    global _presence_cleanup_batch_size
    if _registered:
        return

    _presence_connection_ttl_seconds = _resolve_positive_int(
        current_app.config.get("PRESENCE_CONNECTION_TTL_SECONDS", 75),
        default_value=75,
        minimum=15,
    )
    _presence_heartbeat_interval_seconds = _resolve_positive_int(
        current_app.config.get("PRESENCE_HEARTBEAT_INTERVAL_SECONDS", 20),
        default_value=20,
        minimum=5,
    )
    if _presence_heartbeat_interval_seconds >= _presence_connection_ttl_seconds:
        _presence_heartbeat_interval_seconds = max(5, _presence_connection_ttl_seconds // 2)
    _presence_cleanup_batch_size = _resolve_positive_int(
        current_app.config.get("PRESENCE_CLEANUP_BATCH_SIZE", 200),
        default_value=200,
        minimum=10,
    )

    def _private_pending_batch_size():
        value = current_app.config.get("SOCKET_PENDING_PRIVATE_BATCH_SIZE", 100)
        try:
            return max(1, int(value))
        except (TypeError, ValueError):
            return 100

    def _group_pending_batch_size():
        value = current_app.config.get("SOCKET_PENDING_GROUP_BATCH_SIZE", 100)
        try:
            return max(1, int(value))
        except (TypeError, ValueError):
            return 100

    def _emit_pending_messages_chunk(username):
        batch_size = _private_pending_batch_size()
        pending = message_service.peek_messages_batch(username, limit=batch_size)
        if not pending:
            return 0

        remaining_count = message_service.get_pending_count(username)
        emit("pending_messages", {
            "messages": pending,
            "has_more": remaining_count > len(pending),
            "remaining_count": remaining_count,
        })
        transient_ids = [
            (message or {}).get("message_id")
            for message in pending
            if isinstance((message or {}).get("message_id"), str)
        ]
        if transient_ids:
            message_service.ack_transient_messages(username, transient_ids)
        logger.info(
            "Emitting pending_messages to %s: chunk=%d, remaining=%d",
            username,
            len(pending),
            remaining_count,
        )
        return len(pending)

    def _emit_pending_group_messages_chunk(username, group_id, group_name=None):
        batch_size = _group_pending_batch_size()
        pending_group = message_service.peek_group_messages_for_user(
            username, group_id, limit=batch_size
        )
        if not pending_group:
            return 0

        remaining_count = message_service.get_group_pending_count(username, group_id)
        emit("pending_group_messages", {
            "group_id": group_id,
            "group_name": group_name or "Group Chat",
            "messages": pending_group,
            "has_more": remaining_count > len(pending_group),
            "remaining_count": remaining_count,
        })
        transient_ids = [
            (message or {}).get("message_id")
            for message in pending_group
            if isinstance((message or {}).get("message_id"), str)
        ]
        if transient_ids:
            message_service.ack_group_transient_messages(
                username, group_id, transient_ids
            )
        logger.info(
            "Emitting pending_group_messages to %s for group %s: chunk=%d, remaining=%d",
            username,
            group_id,
            len(pending_group),
            remaining_count,
        )
        return len(pending_group)

    def _sync_group_read_state(username, group_id):
        pending_payloads = message_service.peek_group_messages_for_user(
            username,
            group_id,
            limit=500,
        )
        pending_message_ids = list(
            dict.fromkeys(
                payload.get("message_id")
                for payload in pending_payloads
                if isinstance(payload, dict) and isinstance(payload.get("message_id"), str)
            )
        )
        if not pending_message_ids:
            return 0

        marked, seen_payloads = message_service.mark_group_messages_seen_with_payloads(
            username=username,
            group_id=group_id,
            message_ids=pending_message_ids,
        )
        if marked <= 0 or not seen_payloads:
            return marked

        delivered_by_sender = {}
        seen_by_sender = {}
        offline_events_by_sender = {}

        for payload in seen_payloads:
            sender = payload.get("from")
            message_id = payload.get("message_id")
            if not sender or not message_id or sender == username:
                continue

            seen_by_sender.setdefault(sender, []).append(message_id)
            if payload.get("delivered_now"):
                delivered_by_sender.setdefault(sender, []).append(message_id)

        for sender, message_ids in delivered_by_sender.items():
            delivered_payload = {
                "group_id": int(group_id),
                "message_ids": list(dict.fromkeys(message_ids)),
            }
            socketio.emit(
                "group_delivered_messages_status",
                delivered_payload,
                room=sender,
            )
            if not is_user_online(sender):
                offline_events_by_sender.setdefault(sender, []).append(
                    {
                        "event": "group_delivered_messages_status",
                        "payload": delivered_payload,
                    }
                )

        for sender, message_ids in seen_by_sender.items():
            seen_payload = {
                "group_id": int(group_id),
                "message_ids": list(dict.fromkeys(message_ids)),
            }
            socketio.emit(
                "group_seen_messages_status",
                seen_payload,
                room=sender,
            )
            if not is_user_online(sender):
                offline_events_by_sender.setdefault(sender, []).append(
                    {
                        "event": "group_seen_messages_status",
                        "payload": seen_payload,
                    }
                )

        for sender, events in offline_events_by_sender.items():
            message_service.queue_message_deletion_events_batch(sender, events)

        return marked

    def _emit_pending_delete_events(username):
        pending_events = message_service.pop_message_deletion_events(username)
        for event in pending_events:
            if not isinstance(event, dict):
                continue
            event_name = event.get("event")
            payload = event.get("payload")
            if not event_name or not isinstance(payload, dict):
                continue
            emit(event_name, payload)

    @socketio.on("connect")
    def handle_connect(auth):
        token = _extract_access_token(auth)
        if not token:
            return False

        try:
            claims = decode_token(token)
        except Exception:
            return False

        username = claims.get("sub")
        if not username:
            return False

        session["username"] = username
        join_room(username)
        _register_user_sid(username, request.sid)
        became_online = _set_user_online(username, request.sid)

        _emit_pending_messages_chunk(username)
        _emit_pending_delete_events(username)

        try:
            from app.repositories import user_repository, group_repository

            user = user_repository.get_by_username(username)
            if user:
                user_groups = group_repository.get_groups_for_user(user.id)
                for group in user_groups:
                    group_room_name = f"group_{group.id}"
                    join_room(group_room_name)
                    _track_group_room_join(request.sid, group.id)
                    _emit_pending_group_messages_chunk(
                        username=username,
                        group_id=group.id,
                        group_name=group.name,
                    )
        except Exception as exc:
            logger.warning("Failed to emit pending group messages for %s: %s", username, exc)

        try:
            unread_count = activity_notification_service.get_unread_count(username)
            if unread_count > 0:
                data = activity_notification_service.get_activity_notifications(
                    username, page=1, limit=10, unread_only=True
                )
                emit("pending_activity_notifications", {
                    "unread_count": unread_count,
                    "notifications": data.get("notifications", []),
                })
                logger.info(
                    "Emitting pending_activity_notifications to %s: count=%d",
                    username, unread_count,
                )
        except Exception as exc:
            logger.warning("Failed to emit pending activity notifications for %s: %s", username, exc)

        emit(
            "connected",
            {
                "username": username,
                "presence": {
                    "heartbeat_interval_seconds": _presence_heartbeat_interval_seconds,
                    "timeout_seconds": _presence_connection_ttl_seconds,
                },
            },
        )
        if became_online:
            socketio.emit(
                "user_status",
                {"username": username, "online": True},
                skip_sid=request.sid,
            )
            _emit_group_presence_changed(username, True)

    @socketio.on("disconnect")
    def handle_disconnect():
        username = session.get("username")
        _unregister_user_sid(username, request.sid)
        with _presence_state_lock:
            _sid_group_rooms.pop(request.sid, None)
        if username:
            became_offline = _set_user_offline(username, request.sid)
            if became_offline:
                socketio.emit("user_status", {"username": username, "online": False})
                _emit_group_presence_changed(username, False)

    @socketio.on("presence_heartbeat")
    def handle_presence_heartbeat(_data=None):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        was_online = is_user_online(username)
        _refresh_presence_connection(username, request.sid)
        is_online_now = is_user_online(username)

        emit(
            "presence_heartbeat_ack",
            {
                "username": username,
                "online": is_online_now,
                "server_time": datetime.now(timezone.utc).isoformat(),
                "timeout_seconds": _presence_connection_ttl_seconds,
            },
        )
        if (not was_online) and is_online_now:
            socketio.emit(
                "user_status",
                {"username": username, "online": True},
                skip_sid=request.sid,
            )
            _emit_group_presence_changed(username, True)

    @socketio.on("get_user_status")
    def handle_get_user_status(data):
        if not isinstance(data, dict) or not data.get("username"):
            emit("message_error", {"error": "Invalid payload"})
            return

        target_username = data.get("username")
        emit(
            "user_status",
            {"username": target_username, "online": is_user_online(target_username)},
        )

    @socketio.on("send_message")
    def handle_send_message(data):
        sender = session.get("username")
        if not sender:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        recipient = data.get("to")
        encrypted_message = data.get("message")
        encrypted_key = data.get("encrypted_key")
        attachment = data.get("attachment")
        message_type = data.get("type")
        reply_to_message_id = data.get("reply_to_message_id")
        reply_to_sender = data.get("reply_to_sender")
        encrypted_reply_preview = data.get("encrypted_reply_preview")
        encrypted_reply_key = data.get("encrypted_reply_key")
        sender_encrypted_message = data.get("sender_encrypted_message")
        sender_encrypted_key = data.get("sender_encrypted_key")
        client_message_id, correlation_error = _resolve_client_message_id(
            data.get("client_message_id"),
            required=True,
        )
        if correlation_error:
            emit(
                "message_error",
                {
                    "error": correlation_error,
                    "code": "invalid_client_message_id",
                },
            )
            return

        if attachment is not None and not isinstance(attachment, dict):
            emit("message_error", {"error": "Invalid attachment payload"})
            return

        try:
            payload, created = message_service.send_message_with_status(
                sender,
                recipient,
                encrypted_message,
                encrypted_key,
                attachment=attachment,
                message_type=message_type,
                reply_to_message_id=reply_to_message_id,
                reply_to_sender=reply_to_sender,
                encrypted_reply_preview=encrypted_reply_preview,
                encrypted_reply_key=encrypted_reply_key,
                sender_encrypted_message=sender_encrypted_message,
                sender_encrypted_key=sender_encrypted_key,
                client_message_id=client_message_id,
            )
        except ValueError as exc:
            emit("message_error", {"error": str(exc)})
            return
        except PermissionError as exc:
            emit(
                "message_error",
                {"error": str(exc), "code": "blocked"},
            )
            return

        if created:
            recipient_payload = dict(payload)
            recipient_payload.pop("sender_encrypted_message", None)
            recipient_payload.pop("sender_encrypted_key", None)
            socketio.emit("new_message", recipient_payload, room=recipient)
            logger.debug(
                "Emitting new_message to %s: type=%s, sample=%s",
                recipient,
                type(recipient_payload).__name__,
                str(recipient_payload)[:300],
            )

            if is_user_online(recipient):
                message_service.ack_transient_messages(
                    recipient,
                    [payload["message_id"]],
                )

            recipient_unread = notification_service.get_sender_unread_summary(
                recipient,
                sender,
            )
            sender_unread = notification_service.get_sender_unread_summary(
                sender,
                recipient,
            )

            recipient_contacts_update = _build_contacts_update_payload(
                contact=sender,
                from_username=sender,
                to_username=recipient,
                timestamp=payload["timestamp"],
                message_type=payload["type"],
                has_unread=recipient_unread.get("count", 0) > 0,
                unread_count=recipient_unread.get("count", 0),
                sync_reason="incoming_message",
            )
            sender_contacts_update = _build_contacts_update_payload(
                contact=recipient,
                from_username=sender,
                to_username=recipient,
                timestamp=payload["timestamp"],
                message_type=payload["type"],
                has_unread=sender_unread.get("count", 0) > 0,
                unread_count=sender_unread.get("count", 0),
                sync_reason="outgoing_message",
            )

            socketio.emit("contacts_updated", recipient_contacts_update, room=recipient)
            emit("contacts_updated", sender_contacts_update)

        emit(
            "message_sent",
            {
                "to": recipient,
                "message_id": payload["message_id"],
                "client_message_id": payload.get("client_message_id"),
                "timestamp": payload["timestamp"],
                "type": payload["type"],
            },
        )

    def _private_scope_ignored_payload(scope_result):
        ignored = []
        unknown_ids = scope_result.get("unknown_ids") or []
        wrong_chat_ids = scope_result.get("wrong_chat_ids") or []

        if unknown_ids:
            ignored.append(
                {
                    "reason": "unknown_message_ids",
                    "count": len(unknown_ids),
                    "message_ids": unknown_ids,
                }
            )
        if wrong_chat_ids:
            ignored.append(
                {
                    "reason": "wrong_chat_scope",
                    "count": len(wrong_chat_ids),
                    "message_ids": wrong_chat_ids,
                }
            )
        return ignored

    @socketio.on("ack_messages")
    def handle_ack_messages(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        chat_id = (data.get("chat_id") or "").strip()
        message_ids = data.get("message_ids")
        if not chat_id or not isinstance(message_ids, list) or not message_ids:
            emit("message_error", {"error": "chat_id and message_ids are required"})
            return

        normalized_message_ids = [
            msg_id.strip()
            for msg_id in message_ids
            if isinstance(msg_id, str) and msg_id.strip()
        ]
        if not normalized_message_ids:
            emit("message_error", {"error": "message_ids must contain valid ids"})
            return

        scope_result = message_service.classify_private_message_ids_for_chat(
            recipient=username,
            sender=chat_id,
            message_ids=normalized_message_ids,
        )
        if not scope_result.get("db_verified", False):
            emit(
                "message_error",
                {
                    "error": "Durable message store unavailable for ack validation",
                    "code": "ack_requires_durable_store",
                },
            )
            return

        ack_message_ids = scope_result.get("scoped_ids") or []
        ignored = _private_scope_ignored_payload(scope_result)
        if not ack_message_ids:
            emit(
                "ack_confirmed",
                {
                    "chat_id": chat_id,
                    "removed": 0,
                    "ignored": ignored,
                },
            )
            _emit_pending_messages_chunk(username)
            return

        removed, removed_payloads = message_service.ack_messages_with_payloads(
            username, ack_message_ids
        )
        delivered_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        delivered_ids_by_sender = {}
        for payload in removed_payloads:
            sender = payload.get("from")
            message_id = payload.get("message_id")
            if not sender or not message_id or sender == username:
                continue
            delivered_ids_by_sender.setdefault(sender, []).append(message_id)

        for sender, sender_message_ids in delivered_ids_by_sender.items():
            message_service.mark_private_messages_delivered_batch(
                sender, username, sender_message_ids
            )
            offline_events = []
            sender_online = is_user_online(sender)
            for message_id in sender_message_ids:
                delivered_payload = {
                    "chat_id": username,
                    "message_id": message_id,
                    "delivered_to": username,
                    "delivered_at": delivered_at,
                }
                socketio.emit(
                    "message_delivered",
                    delivered_payload,
                    room=sender,
                )
                if not sender_online:
                    offline_events.append(
                        {
                            "event": "message_delivered",
                            "payload": delivered_payload,
                        }
                    )

            if offline_events:
                message_service.queue_message_deletion_events_batch(
                    sender,
                    offline_events,
                )

        emit(
            "ack_confirmed",
            {
                "chat_id": chat_id,
                "removed": removed,
                "ignored": ignored,
            },
        )
        _emit_pending_messages_chunk(username)

        if chat_id:
            unread_snapshot = notification_service.get_sender_unread_summary(
                username,
                chat_id,
            )
            unread_count = unread_snapshot.get("count", 0)
            emit(
                "contacts_updated",
                _build_contacts_update_payload(
                    contact=chat_id,
                    from_username=chat_id,
                    to_username=username,
                    timestamp=unread_snapshot.get("last_timestamp", ""),
                    message_type=unread_snapshot.get("last_type", "text"),
                    has_unread=unread_count > 0,
                    unread_count=unread_count,
                    sync_reason="ack",
                ),
            )

    @socketio.on("mark_read_messages")
    def handle_mark_read_messages(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        chat_id = (data.get("chat_id") or "").strip()
        message_ids = data.get("message_ids")
        if not chat_id or not isinstance(message_ids, list):
            emit("message_error", {"error": "chat_id and message_ids are required"})
            return

        normalized_ids = [
            msg_id.strip()
            for msg_id in message_ids
            if isinstance(msg_id, str) and msg_id.strip()
        ]
        if not normalized_ids:
            emit("message_error", {"error": "message_ids must contain valid ids"})
            return

        scope_result = message_service.classify_private_message_ids_for_chat(
            recipient=username,
            sender=chat_id,
            message_ids=normalized_ids,
        )
        if not scope_result.get("db_verified", False):
            emit(
                "message_error",
                {
                    "error": "Durable message store unavailable for read validation",
                    "code": "read_requires_durable_store",
                },
            )
            return

        readable_ids = scope_result.get("scoped_ids") or []
        ignored = _private_scope_ignored_payload(scope_result)
        if not readable_ids:
            emit(
                "read_confirmed",
                {
                    "chat_id": chat_id,
                    "marked": 0,
                    "ignored": ignored,
                },
            )
            return

        message_service.mark_private_messages_seen_batch(chat_id, username, readable_ids)
        seen_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        sender_online = is_user_online(chat_id)
        offline_events = []

        for message_id in readable_ids:
            seen_payload = {
                "chat_id": username,
                "message_id": message_id,
                "seen_by": username,
                "seen_at": seen_at,
            }
            socketio.emit(
                "message_seen",
                seen_payload,
                room=chat_id,
            )
            if not sender_online:
                offline_events.append(
                    {
                        "event": "message_seen",
                        "payload": seen_payload,
                    }
                )

        if offline_events:
            message_service.queue_message_deletion_events_batch(
                chat_id,
                offline_events,
            )

        emit(
            "read_confirmed",
            {
                "chat_id": chat_id,
                "marked": len(readable_ids),
                "ignored": ignored,
            },
        )

    @socketio.on("sync_private_chat_read_state")
    def handle_sync_private_chat_read_state(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        chat_id = (data.get("chat_id") or "").strip()
        if not chat_id:
            emit("message_error", {"error": "chat_id is required"})
            return

        synced = message_service.sync_private_chat_read_state(
            recipient=username,
            sender=chat_id,
        )
        synced_ids = synced.get("message_ids") or []
        if synced_ids:
            delivered_payload = {
                "chat_id": username,
                "message_ids": synced_ids,
            }
            seen_payload = {
                "chat_id": username,
                "message_ids": synced_ids,
            }
            socketio.emit(
                "delivered_messages_status",
                delivered_payload,
                room=chat_id,
            )
            socketio.emit(
                "seen_messages_status",
                seen_payload,
                room=chat_id,
            )
            if not is_user_online(chat_id):
                message_service.queue_message_deletion_events_batch(
                    chat_id,
                    [
                        {
                            "event": "delivered_messages_status",
                            "payload": delivered_payload,
                        },
                        {
                            "event": "seen_messages_status",
                            "payload": seen_payload,
                        },
                    ],
                )

        unread_snapshot = notification_service.get_sender_unread_summary(
            username,
            chat_id,
        )
        unread_count = unread_snapshot.get("count", 0)
        emit(
            "contacts_updated",
            _build_contacts_update_payload(
                contact=chat_id,
                from_username=chat_id,
                to_username=username,
                timestamp=unread_snapshot.get("last_timestamp", ""),
                message_type=unread_snapshot.get("last_type", "text"),
                has_unread=unread_count > 0,
                unread_count=unread_count,
                sync_reason="chat_open_sync",
            ),
        )
        emit(
            "private_chat_state_synced",
            {
                "chat_id": chat_id,
                "marked_delivered": int(synced.get("marked_delivered", 0)),
                "marked_seen": int(synced.get("marked_seen", 0)),
            },
        )

    @socketio.on("get_delivered_messages")
    def handle_get_delivered_messages(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        chat_id = (data.get("chat_id") or "").strip()
        message_ids = data.get("message_ids")
        if not chat_id or not isinstance(message_ids, list):
            emit("message_error", {"error": "chat_id and message_ids are required"})
            return

        normalized_ids = [
            msg_id.strip()
            for msg_id in message_ids
            if isinstance(msg_id, str) and msg_id.strip()
        ]
        delivered_ids = message_service.get_private_delivered_message_ids(
            sender=username,
            recipient=chat_id,
            message_ids=normalized_ids,
        )
        emit(
            "delivered_messages_status",
            {
                "chat_id": chat_id,
                "message_ids": delivered_ids,
            },
        )

    @socketio.on("get_seen_messages")
    def handle_get_seen_messages(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        chat_id = (data.get("chat_id") or "").strip()
        message_ids = data.get("message_ids")
        if not chat_id or not isinstance(message_ids, list):
            emit("message_error", {"error": "chat_id and message_ids are required"})
            return

        normalized_ids = [
            msg_id.strip()
            for msg_id in message_ids
            if isinstance(msg_id, str) and msg_id.strip()
        ]
        seen_ids = message_service.get_private_seen_message_ids(
            sender=username,
            recipient=chat_id,
            message_ids=normalized_ids,
        )
        emit(
            "seen_messages_status",
            {
                "chat_id": chat_id,
                "message_ids": seen_ids,
            },
        )

    @socketio.on("join_group")
    def handle_join_group(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict) or not data.get("group_id"):
            emit("message_error", {"error": "group_id is required"})
            return

        group_id = data.get("group_id")

        try:
            from app.repositories import user_repository, group_repository
            user = user_repository.get_by_username(username)
            if not user or not group_repository.is_member(group_id, user.id):
                emit("message_error", {"error": "You are not a member of this group"})
                return
        except Exception as exc:
            logger.warning("join_group membership check failed for %s: %s", username, exc)
            emit("message_error", {"error": "Failed to verify group membership"})
            return

        room_name = f"group_{group_id}"
        join_room(room_name)
        _track_group_room_join(request.sid, group_id)
        emit("group_joined", {"group_id": group_id})
        online_payload = get_group_online_users_payload(group_id)
        if online_payload is not None:
            emit("group_online_users", online_payload)
        marked_as_read = _sync_group_read_state(username=username, group_id=group_id)
        emit(
            "group_read_state_synced",
            {
                "group_id": group_id,
                "marked": marked_as_read,
            },
        )
        logger.debug("User %s joined group room %s", username, room_name)

    @socketio.on("get_group_online_users")
    def handle_get_group_online_users(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict) or not data.get("group_id"):
            emit("message_error", {"error": "group_id is required"})
            return

        group_id = data.get("group_id")
        try:
            from app.repositories import group_repository, user_repository

            user = user_repository.get_by_username(username)
            if not user or not group_repository.is_member(group_id, user.id):
                emit("message_error", {"error": "You are not a member of this group"})
                return
        except Exception as exc:
            logger.warning(
                "get_group_online_users membership check failed for %s: %s",
                username,
                exc,
            )
            emit("message_error", {"error": "Failed to verify group membership"})
            return

        payload = get_group_online_users_payload(group_id)
        if payload is None:
            emit("message_error", {"error": "Failed to load group online users"})
            return
        emit("group_online_users", payload)

    @socketio.on("group_typing")
    def handle_group_typing(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict) or not data.get("group_id"):
            emit("message_error", {"error": "group_id is required"})
            return

        group_id = data.get("group_id")
        is_typing = bool(data.get("is_typing", True))

        try:
            from app.repositories import group_repository, user_repository

            user = user_repository.get_by_username(username)
            if not user or not group_repository.is_member(group_id, user.id):
                emit("message_error", {"error": "You are not a member of this group"})
                return
        except Exception as exc:
            logger.warning("group_typing membership check failed for %s: %s", username, exc)
            emit("message_error", {"error": "Failed to verify group membership"})
            return

        user_payload = _build_group_user_payload(username)
        if not user_payload:
            return

        socketio.emit(
            "group_typing",
            {
                "group_id": int(group_id),
                "is_typing": is_typing,
                "user": user_payload,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            room=f"group_{group_id}",
            skip_sid=request.sid,
        )

    @socketio.on("send_group_message")
    def handle_send_group_message(data):
        sender = session.get("username")
        if not sender:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        group_id = data.get("group_id")
        message_text = data.get("message")
        attachment = data.get("attachment")
        message_type = data.get("type")
        reply_to_message_id = data.get("reply_to_message_id")
        reply_to_sender = data.get("reply_to_sender")
        encrypted_reply_preview = data.get("encrypted_reply_preview")
        encrypted_keys = data.get("encrypted_keys")
        recipient_key_records = data.get("recipient_key_records")
        group_key_ref = data.get("group_key_ref")
        client_message_id, correlation_error = _resolve_client_message_id(
            data.get("client_message_id"),
            required=True,
        )

        def emit_group_message_error(error, code):
            payload = {
                "error": error,
                "code": code,
            }
            if client_message_id:
                payload["client_message_id"] = client_message_id
            emit("message_error", payload)

        if correlation_error:
            emit_group_message_error(correlation_error, "invalid_client_message_id")
            return

        if not group_id:
            emit_group_message_error("group_id is required", "missing_group_id")
            return

        if not message_text and not attachment:
            emit_group_message_error(
                "Message or attachment is required",
                "missing_message_or_attachment",
            )
            return

        if attachment is not None and not isinstance(attachment, dict):
            emit_group_message_error("Invalid attachment payload", "invalid_attachment_payload")
            return

        try:
            from app.repositories import user_repository, group_repository
            from app.repositories import message_repository

            user = user_repository.get_by_username(sender)
            if not user or not group_repository.is_member(group_id, user.id):
                emit_group_message_error(
                    "You are not a member of this group",
                    "not_group_member",
                )
                return
            membership_version = group_repository.get_membership_version(group_id)

            normalized_type = message_type
            if normalized_type:
                normalized_type = normalized_type.strip().lower()
                if normalized_type not in message_service.ALLOWED_MESSAGE_TYPES:
                    emit_group_message_error("Invalid message type", "invalid_message_type")
                    return
            elif attachment and message_text:
                normalized_type = "mixed"
            elif attachment:
                normalized_type = attachment.get("type", "image")
            else:
                normalized_type = "text"

            normalized_recipient_keys = message_repository.normalize_recipient_key_records(
                recipient_key_records
            )
            if not normalized_recipient_keys:
                normalized_recipient_keys = message_repository.normalize_encrypted_keys_map(
                    encrypted_keys
                )

            normalized_group_key_ref = message_repository.normalize_group_key_ref(
                group_key_ref
            )
            if not normalized_recipient_keys:
                if not normalized_group_key_ref:
                    emit_group_message_error(
                        "recipient_key_records or group_key_ref is required",
                        "missing_group_key_material",
                    )
                    return
                persisted_keys = message_repository.get_group_key_record_map(
                    group_id=group_id,
                    sender=sender,
                    group_key_ref=normalized_group_key_ref,
                )
                if not persisted_keys:
                    emit_group_message_error(
                        "Unknown group_key_ref for sender",
                        "unknown_group_key_ref",
                    )
                    return

            if client_message_id:
                existing_payload = message_service.get_group_message_by_client_message_id(
                    sender=sender,
                    group_id=group_id,
                    client_message_id=client_message_id,
                )
                if existing_payload is not None:
                    emit(
                        "group_message_sent",
                        {
                            "group_id": group_id,
                            "message_id": existing_payload["message_id"],
                            "client_message_id": existing_payload.get("client_message_id"),
                            "timestamp": existing_payload["timestamp"],
                            "type": existing_payload["type"],
                        },
                    )
                    return

            payload = message_repository.build_group_message_payload(
                sender=sender,
                group_id=group_id,
                encrypted_message=message_text,
                attachment=attachment,
                message_type=normalized_type,
                reply_to_message_id=reply_to_message_id,
                reply_to_sender=reply_to_sender,
                encrypted_reply_preview=encrypted_reply_preview,
                encrypted_keys=normalized_recipient_keys,
                recipient_key_records=recipient_key_records,
                group_key_ref=normalized_group_key_ref,
                client_message_id=client_message_id,
            )

            side_effects_enqueued = async_task_service.enqueue_group_message_side_effects(
                sender=sender,
                group_id=group_id,
                message_payload=payload,
                expected_membership_version=membership_version,
                source="socket.send_group_message",
            )
            if not side_effects_enqueued:
                if async_task_service.should_fallback_inline(
                    task_type=async_task_service.TASK_TYPE_GROUP_MESSAGE_SIDE_EFFECTS
                ):
                    group_notification_service.dispatch_group_message_side_effects(
                        sender=sender,
                        group_id=group_id,
                        message_payload=payload,
                        expected_membership_version=membership_version,
                    )
                else:
                    logger.error(
                        "Group side effects unavailable sender=%s group_id=%s message_id=%s",
                        sender,
                        group_id,
                        payload.get("message_id"),
                    )
                    emit_group_message_error(
                        "Group messaging is temporarily unavailable. Please retry.",
                        "group_side_effects_unavailable",
                    )
                    return

            canonical_payload, created = message_service.store_group_message_metadata(
                payload,
                group_id,
            )
            if not created:
                emit(
                    "group_message_sent",
                    {
                        "group_id": group_id,
                        "message_id": canonical_payload["message_id"],
                        "client_message_id": canonical_payload.get("client_message_id"),
                        "timestamp": canonical_payload["timestamp"],
                        "type": canonical_payload["type"],
                    },
                )
                return

            message_repository.record_group_conversation_timestamp(
                group_id,
                canonical_payload.get("timestamp"),
            )

            member_usernames = list(dict.fromkeys(
                group_repository.get_group_member_usernames(group_id)
            ))
            delivery_guard = GroupDeliveryGuard(
                group_id,
                expected_membership_version=membership_version,
            )
            delivered_count = 0
            recipient_payloads = message_repository.build_group_message_payloads_for_recipients(
                canonical_payload,
                member_usernames,
            )
            for member_username in member_usernames:
                if not delivery_guard.can_dispatch_to(member_username):
                    continue
                authorized_sids = _authorized_group_sids(member_username, group_id)
                if not authorized_sids:
                    continue
                recipient_payload = recipient_payloads.get(member_username)
                if not isinstance(recipient_payload, dict):
                    continue
                for sid in authorized_sids:
                    socketio.emit("new_group_message", recipient_payload, room=sid)
                delivered_count += 1
            logger.debug(
                "Emitting new_group_message for group %s from %s to %d members",
                group_id, sender, delivered_count,
            )

            emit("group_message_sent", {
                "group_id": group_id,
                "message_id": canonical_payload["message_id"],
                "client_message_id": canonical_payload.get("client_message_id"),
                "timestamp": canonical_payload["timestamp"],
                "type": canonical_payload["type"],
            })

        except Exception as exc:
            logger.error("send_group_message error: %s", exc, exc_info=True)
            emit_group_message_error("Failed to send group message", "group_send_failed")

    @socketio.on("get_contacts_status")
    def handle_get_contacts_status(data=None):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        page = 1
        limit = 20
        if isinstance(data, dict):
            try:
                page = int(data.get("page", 1))
            except (TypeError, ValueError):
                page = 1
            try:
                limit = int(data.get("limit", 20))
            except (TypeError, ValueError):
                limit = 20

        from app.services import contact_service
        result = contact_service.get_contacts_with_message_status(
            username, page=page, limit=limit
        )
        emit("contacts_status", result)

    @socketio.on("ack_group_messages")
    def handle_ack_group_messages(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        group_id = data.get("group_id")
        message_ids = data.get("message_ids")
        if not group_id:
            emit("message_error", {"error": "group_id is required"})
            return
        if not isinstance(message_ids, list) or not message_ids:
            emit("message_error", {"error": "message_ids must be a non-empty list"})
            return

        from app.repositories import user_repository, group_repository
        normalized_message_ids = [
            msg_id.strip()
            for msg_id in message_ids
            if isinstance(msg_id, str) and msg_id.strip()
        ]
        if not normalized_message_ids:
            emit("message_error", {"error": "message_ids must contain valid ids"})
            return

        user = user_repository.get_by_username(username)
        if not user or not group_repository.is_member(group_id, user.id):
            emit("message_error", {"error": "You are not a member of this group"})
            return

        removed, removed_payloads = message_service.ack_group_messages_with_payloads(
            username, group_id, normalized_message_ids
        )
        delivered_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        delivered_pairs = [
            (payload.get("from"), payload.get("message_id"))
            for payload in removed_payloads
            if payload.get("message_id") and payload.get("from") != username
        ]
        for sender, message_id in delivered_pairs:
            socketio.emit(
                "group_message_delivered",
                {
                    "group_id": group_id,
                    "message_id": message_id,
                    "delivered_to": username,
                    "delivered_at": delivered_at,
                },
                room=sender,
            )

        emit("ack_group_confirmed", {"group_id": group_id, "removed": removed})
        group = group_repository.get_group_by_id(group_id)
        _emit_pending_group_messages_chunk(
            username=username,
            group_id=group_id,
            group_name=group.name if group else "Group Chat",
        )

    @socketio.on("mark_read_group_messages")
    def handle_mark_read_group_messages(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        group_id = data.get("group_id")
        message_ids = data.get("message_ids")
        if not group_id or not isinstance(message_ids, list) or not message_ids:
            emit("message_error", {"error": "group_id and message_ids are required"})
            return

        from app.repositories import user_repository, group_repository

        normalized_message_ids = [
            msg_id.strip()
            for msg_id in message_ids
            if isinstance(msg_id, str) and msg_id.strip()
        ]
        if not normalized_message_ids:
            emit("message_error", {"error": "message_ids must contain valid ids"})
            return

        user = user_repository.get_by_username(username)
        if not user or not group_repository.is_member(group_id, user.id):
            emit("message_error", {"error": "You are not a member of this group"})
            return

        marked, seen_payloads = message_service.mark_group_messages_seen_with_payloads(
            username=username,
            group_id=group_id,
            message_ids=normalized_message_ids,
        )
        if marked <= 0:
            emit("group_read_confirmed", {"group_id": group_id, "marked": 0})
            return

        seen_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        delivered_at = seen_at
        for payload in seen_payloads:
            sender = payload.get("from")
            message_id = payload.get("message_id")
            if not sender or not message_id or sender == username:
                continue

            if payload.get("delivered_now"):
                socketio.emit(
                    "group_message_delivered",
                    {
                        "group_id": group_id,
                        "message_id": message_id,
                        "delivered_to": username,
                        "delivered_at": delivered_at,
                    },
                    room=sender,
                )

            socketio.emit(
                "group_message_seen",
                {
                    "group_id": group_id,
                    "message_id": message_id,
                    "seen_by": username,
                    "seen_at": seen_at,
                },
                room=sender,
            )

        emit("group_read_confirmed", {"group_id": group_id, "marked": marked})

    @socketio.on("get_group_delivered_messages")
    def handle_get_group_delivered_messages(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        group_id = data.get("group_id")
        message_ids = data.get("message_ids")
        if not group_id or not isinstance(message_ids, list):
            emit("message_error", {"error": "group_id and message_ids are required"})
            return

        from app.repositories import user_repository, group_repository

        user = user_repository.get_by_username(username)
        if not user or not group_repository.is_member(group_id, user.id):
            emit("message_error", {"error": "You are not a member of this group"})
            return

        normalized_ids = [
            msg_id.strip()
            for msg_id in message_ids
            if isinstance(msg_id, str) and msg_id.strip()
        ]
        delivered_ids = message_service.get_group_delivered_message_ids(
            group_id=group_id,
            message_ids=normalized_ids,
            sender_username=username,
        )
        emit(
            "group_delivered_messages_status",
            {
                "group_id": group_id,
                "message_ids": delivered_ids,
            },
        )

    @socketio.on("get_group_seen_messages")
    def handle_get_group_seen_messages(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        group_id = data.get("group_id")
        message_ids = data.get("message_ids")
        if not group_id or not isinstance(message_ids, list):
            emit("message_error", {"error": "group_id and message_ids are required"})
            return

        from app.repositories import user_repository, group_repository

        user = user_repository.get_by_username(username)
        if not user or not group_repository.is_member(group_id, user.id):
            emit("message_error", {"error": "You are not a member of this group"})
            return

        normalized_ids = [
            msg_id.strip()
            for msg_id in message_ids
            if isinstance(msg_id, str) and msg_id.strip()
        ]
        seen_ids = message_service.get_group_seen_message_ids(
            group_id=group_id,
            message_ids=normalized_ids,
            sender_username=username,
        )
        emit(
            "group_seen_messages_status",
            {
                "group_id": group_id,
                "message_ids": seen_ids,
            },
        )

    @socketio.on("delete_message")
    def handle_delete_message(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        message_id = (data.get("message_id") or "").strip()
        chat_id = (data.get("chat_id") or "").strip()
        if not message_id or not chat_id:
            emit("message_error", {"error": "message_id and chat_id are required"})
            return

        meta = message_service.get_message_metadata(message_id)
        if not meta or meta.get("type") != "private":
            emit("message_error", {"error": "Message not found"})
            return

        sender = meta.get("sender")
        recipient = meta.get("recipient")
        if username not in {sender, recipient}:
            emit("message_error", {"error": "Not allowed to delete this message"})
            return
        counterpart = recipient if sender == username else sender

        if not counterpart:
            emit("message_error", {"error": "Unable to resolve message recipient"})
            return
        if counterpart != chat_id:
            emit("message_error", {"error": "Message not found in this chat"})
            return

        message_service.ack_messages(counterpart, [message_id])
        message_service.mark_private_message_deleted(
            username=counterpart,
            chat_id=username,
            message_id=message_id,
        )
        payload = {
            "chat_id": username,
            "message_id": message_id,
            "deleted_by": username,
            "deleted_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }
        socketio.emit("message_deleted", payload, room=counterpart)
        socketio.emit("message_deleted", payload, room=username)
        message_service.queue_message_deletion_event(counterpart, "message_deleted", payload)
        message_service.queue_message_deletion_event(username, "message_deleted", payload)
        message_service.delete_message_metadata(message_id)
        emit("message_delete_confirmed", {"message_id": message_id, "chat_id": counterpart})

    @socketio.on("delete_message_for_me")
    def handle_delete_message_for_me(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        message_id = (data.get("message_id") or "").strip()
        chat_id = (data.get("chat_id") or "").strip()
        if not message_id or not chat_id:
            emit("message_error", {"error": "message_id and chat_id are required"})
            return

        meta = message_service.get_message_metadata(message_id)
        if not meta or meta.get("type") != "private":
            emit("message_error", {"error": "Message not found"})
            return

        sender = meta.get("sender")
        recipient = meta.get("recipient")
        if username not in {sender, recipient}:
            emit("message_error", {"error": "Not allowed to delete this message"})
            return
        counterpart = recipient if sender == username else sender
        if counterpart != chat_id:
            emit("message_error", {"error": "Message not found in this chat"})
            return

        deleted = message_service.mark_private_message_deleted_for_user(
            username=username,
            chat_id=counterpart,
            message_id=message_id,
        )
        if not deleted:
            emit("message_error", {"error": "Message not found in this chat"})
            return

        payload = {
            "chat_id": counterpart,
            "message_id": message_id,
            "deleted_by": username,
            "deleted_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }
        socketio.emit("message_deleted_for_me", payload, room=username)
        message_service.queue_message_deletion_event(username, "message_deleted_for_me", payload)
        emit("message_delete_for_me_confirmed", {"message_id": message_id, "chat_id": counterpart})

    @socketio.on("get_deleted_messages")
    def handle_get_deleted_messages(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        chat_id = (data.get("chat_id") or "").strip()
        message_ids = data.get("message_ids")
        if not chat_id or not isinstance(message_ids, list):
            emit("message_error", {"error": "chat_id and message_ids are required"})
            return

        normalized_ids = [
            msg_id.strip()
            for msg_id in message_ids
            if isinstance(msg_id, str) and msg_id.strip()
        ]
        deleted_ids = message_service.get_private_deleted_message_ids(
            username=username,
            chat_id=chat_id,
            message_ids=normalized_ids,
        )
        emit(
            "deleted_messages_status",
            {
                "chat_id": chat_id,
                "message_ids": deleted_ids,
            },
        )

    @socketio.on("delete_group_message")
    def handle_delete_group_message(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        group_id = data.get("group_id")
        message_id = (data.get("message_id") or "").strip()
        if not group_id or not message_id:
            emit("message_error", {"error": "group_id and message_id are required"})
            return

        from app.repositories import user_repository, group_repository, message_repository

        user = user_repository.get_by_username(username)
        if not user or not group_repository.is_member(group_id, user.id):
            emit("message_error", {"error": "You are not a member of this group"})
            return

        meta = message_service.get_message_metadata(message_id)
        if not meta or int(meta.get("group_id") or -1) != int(group_id):
            emit("message_error", {"error": "Message not found in this group"})
            return
        if meta.get("sender") != username:
            emit("message_error", {"error": "Only sender can delete for everyone"})
            return

        member_usernames = group_repository.get_group_member_usernames(group_id)
        for member_username in member_usernames:
            message_repository.ack_group_messages(member_username, group_id, [message_id])

        payload = {
            "group_id": group_id,
            "message_id": message_id,
            "deleted_by": username,
            "deleted_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }
        emit_group_event_to_members(
            group_id=group_id,
            event_name="group_message_deleted",
            payload=payload,
        )

        for member_username in member_usernames:
            if member_username == username:
                continue
            message_service.mark_group_message_deleted(
                username=member_username,
                group_id=group_id,
                message_id=message_id,
            )
            message_service.queue_message_deletion_event(
                member_username,
                "group_message_deleted",
                payload,
            )

        message_service.delete_message_metadata(message_id)
        emit("group_message_delete_confirmed", {"group_id": group_id, "message_id": message_id})

    @socketio.on("delete_group_message_for_me")
    def handle_delete_group_message_for_me(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        group_id = data.get("group_id")
        message_id = (data.get("message_id") or "").strip()
        if not group_id or not message_id:
            emit("message_error", {"error": "group_id and message_id are required"})
            return

        from app.repositories import user_repository, group_repository

        user = user_repository.get_by_username(username)
        if not user or not group_repository.is_member(group_id, user.id):
            emit("message_error", {"error": "You are not a member of this group"})
            return

        meta = message_service.get_message_metadata(message_id)
        if not meta or int(meta.get("group_id") or -1) != int(group_id):
            emit("message_error", {"error": "Message not found in this group"})
            return

        deleted = message_service.mark_group_message_deleted_for_user(
            username=username,
            group_id=group_id,
            message_id=message_id,
        )
        if not deleted:
            emit("message_error", {"error": "Message not found in this group"})
            return

        payload = {
            "group_id": int(group_id),
            "message_id": message_id,
            "deleted_by": username,
            "deleted_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }
        socketio.emit("group_message_deleted_for_me", payload, room=username)
        message_service.queue_message_deletion_event(
            username,
            "group_message_deleted_for_me",
            payload,
        )
        emit("group_message_delete_for_me_confirmed", {"group_id": group_id, "message_id": message_id})

    @socketio.on("get_group_deleted_messages")
    def handle_get_group_deleted_messages(data):
        username = session.get("username")
        if not username:
            emit("message_error", {"error": "Unauthorized"})
            return

        if not isinstance(data, dict):
            emit("message_error", {"error": "Invalid payload"})
            return

        group_id = data.get("group_id")
        message_ids = data.get("message_ids")
        if not group_id or not isinstance(message_ids, list):
            emit("message_error", {"error": "group_id and message_ids are required"})
            return

        from app.repositories import user_repository, group_repository

        user = user_repository.get_by_username(username)
        if not user or not group_repository.is_member(group_id, user.id):
            emit("message_error", {"error": "You are not a member of this group"})
            return

        normalized_ids = [
            msg_id.strip()
            for msg_id in message_ids
            if isinstance(msg_id, str) and msg_id.strip()
        ]
        deleted_ids = message_service.get_group_deleted_message_ids(
            username=username,
            group_id=group_id,
            message_ids=normalized_ids,
        )
        emit(
            "group_deleted_messages_status",
            {
                "group_id": group_id,
                "message_ids": deleted_ids,
            },
        )

    _start_presence_maintenance_loop()
    _registered = True
