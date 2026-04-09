"""
Quick benchmark for unread summary hot path.

Run:
    python3 tests/benchmark_notification_summary.py
"""

import json
import random
import time


def old_scan_summary(raw_messages):
    per_sender = {}
    for raw in raw_messages:
        msg = json.loads(raw)
        sender = msg.get("from")
        if not sender:
            continue
        entry = per_sender.get(sender)
        if entry is None:
            per_sender[sender] = {
                "count": 1,
                "last_type": msg.get("type", "text"),
                "last_timestamp": msg.get("timestamp", ""),
            }
        else:
            entry["count"] += 1
            entry["last_type"] = msg.get("type", "text")
            entry["last_timestamp"] = msg.get("timestamp", "")
    return per_sender


def metadata_summary(unread_hash, last_map):
    per_sender = {}
    for sender, count in unread_hash.items():
        if count <= 0:
            continue
        last = last_map.get(sender, {})
        per_sender[sender] = {
            "count": count,
            "last_type": last.get("type", "text"),
            "last_timestamp": last.get("timestamp", ""),
        }
    return per_sender


def main():
    sender_count = 64
    message_count = 50_000
    senders = [f"u{i}" for i in range(sender_count)]

    raw_messages = []
    unread_hash = {}
    last_map = {}

    for i in range(message_count):
        sender = random.choice(senders)
        payload = {
            "from": sender,
            "type": "text",
            "timestamp": f"2026-04-07T00:{i % 60:02d}:{i % 60:02d}.000000Z",
            "message_id": f"m-{i}",
        }
        raw_messages.append(json.dumps(payload))
        unread_hash[sender] = unread_hash.get(sender, 0) + 1
        last_map[sender] = {
            "type": payload["type"],
            "timestamp": payload["timestamp"],
        }

    loops = 20

    t0 = time.perf_counter()
    for _ in range(loops):
        old_scan_summary(raw_messages)
    t1 = time.perf_counter()

    t2 = time.perf_counter()
    for _ in range(loops):
        metadata_summary(unread_hash, last_map)
    t3 = time.perf_counter()

    old_ms = (t1 - t0) * 1000 / loops
    new_ms = (t3 - t2) * 1000 / loops
    speedup = old_ms / new_ms if new_ms else float("inf")

    print(f"messages={message_count} senders={sender_count} loops={loops}")
    print(f"old_scan_avg_ms={old_ms:.2f}")
    print(f"metadata_avg_ms={new_ms:.2f}")
    print(f"speedup_x={speedup:.2f}")


if __name__ == "__main__":
    main()
