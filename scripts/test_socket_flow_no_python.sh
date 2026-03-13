#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-http://127.0.0.1:5000}"
PASSWORD="${TEST_PASSWORD:-pass12345}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-20}"
SOCKET_PATH="${SOCKET_PATH:-socket.io}"

suffix="$(date +%s)"
ALICE_USER="sock_alice_${suffix}"
BOB_USER="sock_bob_${suffix}"

url_encode() {
  local raw="$1"
  printf '%s' "$raw" | sed 's/:/%3A/g; s/\//%2F/g; s/+/%2B/g; s/=/%3D/g'
}

extract_access_token() {
  # Minimal JSON token extraction without jq/python.
  # Expects: {"access_token":"...","refresh_token":"..."}
  printf '%s' "$1" | sed -n 's/.*"access_token"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p'
}

extract_sid() {
  # Engine.IO open packet starts with: 0{"sid":"...",...}
  printf '%s' "$1" | sed -n 's/^0{"sid":"\([^"]*\)".*/\1/p'
}

eio_get() {
  local token="$1"
  local sid="$2"
  local tstamp
  tstamp="$(date +%s%N)"

  if [ -n "$sid" ]; then
    curl -sS -H "Authorization: Bearer ${token}" \
      "${BASE_URL}/${SOCKET_PATH}/?EIO=4&transport=polling&sid=${sid}&t=${tstamp}"
  else
    curl -sS -H "Authorization: Bearer ${token}" \
      "${BASE_URL}/${SOCKET_PATH}/?EIO=4&transport=polling&t=${tstamp}"
  fi
}

eio_post() {
  local token="$1"
  local sid="$2"
  local payload="$3"

  curl -sS -X POST \
    -H "Authorization: Bearer ${token}" \
    -H "Content-Type: text/plain;charset=UTF-8" \
    --data-binary "$payload" \
    "${BASE_URL}/${SOCKET_PATH}/?EIO=4&transport=polling&sid=${sid}"
}

echo "[1/7] Registering users"
curl -sS -X POST "${BASE_URL}/api/auth/register" -H "Content-Type: application/json" \
  -d "{\"username\":\"${ALICE_USER}\",\"password\":\"${PASSWORD}\",\"public_key\":\"${ALICE_USER}_pub\"}" >/dev/null || true
curl -sS -X POST "${BASE_URL}/api/auth/register" -H "Content-Type: application/json" \
  -d "{\"username\":\"${BOB_USER}\",\"password\":\"${PASSWORD}\",\"public_key\":\"${BOB_USER}_pub\"}" >/dev/null || true

echo "[2/7] Logging in"
alice_login="$(curl -sS -X POST "${BASE_URL}/api/auth/login" -H "Content-Type: application/json" -d "{\"username\":\"${ALICE_USER}\",\"password\":\"${PASSWORD}\"}")"
bob_login="$(curl -sS -X POST "${BASE_URL}/api/auth/login" -H "Content-Type: application/json" -d "{\"username\":\"${BOB_USER}\",\"password\":\"${PASSWORD}\"}")"

ALICE_TOKEN="$(extract_access_token "$alice_login")"
BOB_TOKEN="$(extract_access_token "$bob_login")"

if [ -z "$ALICE_TOKEN" ] || [ -z "$BOB_TOKEN" ]; then
  echo "ERROR: Could not parse access tokens."
  echo "alice_login=${alice_login}"
  echo "bob_login=${bob_login}"
  exit 1
fi

echo "[3/7] Opening Engine.IO polling sessions"
alice_open="$(eio_get "$ALICE_TOKEN" "")"
bob_open="$(eio_get "$BOB_TOKEN" "")"

ALICE_SID="$(extract_sid "$alice_open")"
BOB_SID="$(extract_sid "$bob_open")"

if [ -z "$ALICE_SID" ] || [ -z "$BOB_SID" ]; then
  echo "ERROR: Could not open socket polling session."
  echo "alice_open=${alice_open}"
  echo "bob_open=${bob_open}"
  exit 1
fi

echo "[4/7] Connecting to Socket.IO namespace"
# 40 = Socket.IO CONNECT packet on default namespace
eio_post "$ALICE_TOKEN" "$ALICE_SID" '40' >/dev/null
eio_post "$BOB_TOKEN" "$BOB_SID" '40' >/dev/null

# Drain initial packets (connected/pending/etc.)
eio_get "$ALICE_TOKEN" "$ALICE_SID" >/dev/null || true
eio_get "$BOB_TOKEN" "$BOB_SID" >/dev/null || true

echo "[5/7] Sending message over Socket.IO"
MESSAGE_TEXT="socket-flow-test-message"
packet="42[\"send_message\",{\"to\":\"${BOB_USER}\",\"message\":\"${MESSAGE_TEXT}\",\"encrypted_key\":\"socket-flow-test-key\",\"type\":\"text\"}]"
eio_post "$ALICE_TOKEN" "$ALICE_SID" "$packet" >/dev/null

echo "[6/7] Waiting for message_sent/new_message events"
alice_ok=0
bob_ok=0
end_ts=$(( $(date +%s) + TIMEOUT_SECONDS ))

while [ "$(date +%s)" -lt "$end_ts" ]; do
  aresp="$(eio_get "$ALICE_TOKEN" "$ALICE_SID" || true)"
  bresp="$(eio_get "$BOB_TOKEN" "$BOB_SID" || true)"

  echo "$aresp" | grep -q '"message_sent"' && alice_ok=1 || true
  echo "$bresp" | grep -q '"new_message"' && echo "$bresp" | grep -q "$MESSAGE_TEXT" && bob_ok=1 || true

  if [ "$alice_ok" -eq 1 ] && [ "$bob_ok" -eq 1 ]; then
    break
  fi
  sleep 1
done

if [ "$alice_ok" -ne 1 ] || [ "$bob_ok" -ne 1 ]; then
  echo "ERROR: Socket events not observed in time."
  echo "alice_ok=${alice_ok}, bob_ok=${bob_ok}"
  exit 1
fi

echo "[7/7] Verifying recipient inbox is empty (online delivery)"
inbox_response="$(curl -sS -X GET "${BASE_URL}/api/messages/inbox" -H "Authorization: Bearer ${BOB_TOKEN}")"
if echo "$inbox_response" | grep -q '"messages"[[:space:]]*:[[:space:]]*\[\]'; then
  echo "Inbox check OK"
else
  echo "WARNING: inbox not empty or response unexpected: ${inbox_response}"
fi

echo "Socket flow OK (no Python client needed)"
echo "Created users: ${ALICE_USER}, ${BOB_USER}"
