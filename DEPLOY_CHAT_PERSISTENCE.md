# Deploy: Durable Chat Message Persistence

This release adds durable chat message storage in SQL (`private_messages`, `group_messages`, `group_message_recipients`) and keeps Redis as transient delivery/cache.

## 1) Deploy order

1. Deploy backend code.
2. Install/update Python dependencies.
3. Run one-time backfill from Redis transient queues (recommended).
4. Restart backend processes.
5. Run smoke checks.

## 2) Commands

From `backend/sec_message_backend`:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python migrate_chat_message_persistence.py
```

Restart app services (example):

```bash
sudo systemctl restart sec-message-backend
sudo systemctl restart sec-message-async-worker
```

## 3) Why backfill is recommended

- During older versions, pending payloads lived only in Redis inbox keys.
- The migration script copies any remaining transient pending payloads into SQL so they survive process restarts.
- If you skip this script, runtime lazy hydration still backfills per-user/per-group when those queues are read, but one-time pre-backfill is safer and easier to observe.

## 4) Post-deploy smoke checks

1. Send private message while recipient is offline, restart backend, reconnect recipient: message should still arrive.
2. Send group message while member is offline, restart backend, reconnect member: message should still arrive.
3. Read history endpoint:
   - `GET /api/messages/history/private/<chat_id>`
   - `GET /api/messages/history/group/<group_id>`

## 5) Rollback note

- Rolling back app code is safe.
- New SQL tables are additive and can remain in DB.
- Redis transient keys are still compatible with older behavior.
