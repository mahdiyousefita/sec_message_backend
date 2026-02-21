from app.services import follow_service


def add_contact(username, contact):
    if not isinstance(contact, str) or not contact.strip():
        raise ValueError("Contact not found")

    try:
        follow_service.follow_by_username(username, contact.strip())
    except ValueError as e:
        if str(e) == "User not found":
            raise ValueError("Contact not found")
        raise


def get_contacts(username):
    return follow_service.get_following_for_username(username)
