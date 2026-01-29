from app.repositories import user_repository, message_repository

def add_contact(username, contact):
    if not user_repository.get_by_username(contact):
        raise ValueError("Contact not found")

    message_repository.add_contact(username, contact)

def get_contacts(username):
    return message_repository.get_contacts(username)
