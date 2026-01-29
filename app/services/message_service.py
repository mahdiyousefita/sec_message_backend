from app.repositories import user_repository, message_repository

def send_message(sender, recipient, message, encrypted_key):
    if not user_repository.get_by_username(recipient):
        raise ValueError("Recipient not found")

    message_repository.push_message(
        sender,
        recipient,
        message,
        encrypted_key
    )

def receive_messages(username):
    return message_repository.pop_messages(username)
