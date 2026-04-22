from app.extensions.extensions import socketio
from app.repositories import group_repository, message_repository
from app.services.group_delivery_guard import GroupDeliveryGuard


def dispatch_group_message_side_effects(
    *,
    sender: str,
    group_id: int,
    message_payload: dict,
    expected_membership_version: int | None = None,
) -> int:
    if not sender or not group_id or not isinstance(message_payload, dict):
        return 0

    normalized_sender = sender.strip() if isinstance(sender, str) else ""
    if not normalized_sender:
        return 0

    group = group_repository.get_group_by_id(group_id)
    group_name = group.name if group else "Group Chat"
    member_usernames = group_repository.get_group_member_usernames(group_id)
    delivery_guard = GroupDeliveryGuard(
        group_id,
        expected_membership_version=expected_membership_version,
    )

    eligible_recipients = []
    for member_username in member_usernames:
        if member_username == normalized_sender:
            continue
        if not delivery_guard.can_dispatch_to(member_username):
            continue
        eligible_recipients.append(member_username)

    recipient_payloads = message_repository.build_group_message_payloads_for_recipients(
        message_payload,
        eligible_recipients,
    )
    deliverable_recipients = list(recipient_payloads.keys())

    for member_username in deliverable_recipients:
        socketio.emit(
            "new_notification",
            {
                "from": normalized_sender,
                "group_id": group_id,
                "group_name": group_name,
                "type": message_payload.get("type", "text"),
                "timestamp": message_payload.get("timestamp", ""),
                "message_id": message_payload.get("message_id", ""),
            },
            room=member_username,
        )

    return message_repository.push_group_messages_to_members(
        group_id=group_id,
        recipients=deliverable_recipients,
        payload=message_payload,
    )


def process_group_message_side_effects_task(payload: dict):
    sender = payload.get("sender")
    group_id = payload.get("group_id")
    message_payload = payload.get("message_payload")
    expected_membership_version = payload.get("expected_membership_version")

    try:
        group_id = int(group_id)
    except (TypeError, ValueError):
        return 0

    return dispatch_group_message_side_effects(
        sender=sender,
        group_id=group_id,
        message_payload=message_payload if isinstance(message_payload, dict) else {},
        expected_membership_version=expected_membership_version,
    )
