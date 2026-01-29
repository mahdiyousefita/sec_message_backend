import json
from datetime import datetime
from app.extensions.redis_client import redis_client

def add_contact(username, contact):
    redis_client.sadd(f"contacts:{username}", contact)

def get_contacts(username):
    return list(redis_client.smembers(f"contacts:{username}"))

def push_message(sender, recipient, encrypted_message, encrypted_key):
    data = json.dumps({
        "from": sender,
        "message": encrypted_message,
        "encrypted_key": encrypted_key,
        "timestamp": datetime.utcnow().isoformat()
    })
    redis_client.rpush(f"inbox:{recipient}", data)

def pop_messages(username):
    key = f"inbox:{username}"
    messages = []

    while True:
        msg = redis_client.lpop(key)
        if not msg:
            break
        messages.append(json.loads(msg))

    return messages
