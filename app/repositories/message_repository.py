import json
from datetime import datetime
from app.extensions.redis_client import redis_client

def add_contact(username, contact):
    redis_client.sadd(f"contacts:{username}", contact)

def get_contacts(username):
    return list(redis_client.smembers(f"contacts:{username}"))

def build_message_payload(sender, encrypted_message, encrypted_key):
    return {
        "from": sender,
        "message": encrypted_message,
        "encrypted_key": encrypted_key,
        "timestamp": datetime.utcnow().isoformat()
    }

def push_message_payload(recipient, payload):
    data = json.dumps(payload)
    redis_client.rpush(f"inbox:{recipient}", data)

def pop_messages(username):
    key = f"inbox:{username}"

    messages = redis_client.lrange(key, 0, -1)
    redis_client.delete(key)

    return [json.loads(msg) for msg in messages]
