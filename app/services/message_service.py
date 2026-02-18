from app.repositories import user_repository, message_repository

def send_message(sender, recipient, message, encrypted_key, persist=True):
    if not user_repository.get_by_username(recipient):
        raise ValueError("Recipient not found")

    if not message:
        raise ValueError("Message is required")

    if not encrypted_key:
        raise ValueError("Encrypted key is required")

    payload = message_repository.build_message_payload(
        sender,
        message,
        encrypted_key
    )
    if persist:
        message_repository.push_message_payload(recipient, payload)

    return payload

def receive_messages(username):
    return message_repository.pop_messages(username)
