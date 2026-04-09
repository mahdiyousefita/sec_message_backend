from app.extensions.extensions import socketio
from app.repositories import group_repository, message_repository


def dispatch_group_message_side_effects(
    *,
    sender: str,
    group_id: int,
    message_payload: dict,
) -> int:
    if not sender or not group_id or not isinstance(message_payload, dict):
        return 0

    group = group_repository.get_group_by_id(group_id)
    group_name = group.name if group else "Group Chat"
    member_usernames = group_repository.get_group_member_usernames(group_id)

    dispatched = 0
    for member_username in member_usernames:
        if member_username == sender:
            continue

        socketio.emit(
            "new_notification",
            {
                "from": sender,
                "group_id": group_id,
                "group_name": group_name,
                "type": message_payload.get("type", "text"),
                "timestamp": message_payload.get("timestamp", ""),
                "message_id": message_payload.get("message_id", ""),
            },
            room=member_username,
        )
        message_repository.push_group_message_to_member(
            group_id,
            member_username,
            message_payload,
        )
        dispatched += 1

    return dispatched


def process_group_message_side_effects_task(payload: dict):
    sender = payload.get("sender")
    group_id = payload.get("group_id")
    message_payload = payload.get("message_payload")

    try:
        group_id = int(group_id)
    except (TypeError, ValueError):
        return 0

    return dispatch_group_message_side_effects(
        sender=sender,
        group_id=group_id,
        message_payload=message_payload if isinstance(message_payload, dict) else {},
    )
