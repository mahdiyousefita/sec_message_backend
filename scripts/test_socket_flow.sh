#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-http://127.0.0.1:5000}"
PASSWORD="${TEST_PASSWORD:-pass12345}"
SOCKET_PATH="${SOCKET_PATH:-socket.io}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-20}"

PYTHON_BIN="${PYTHON_BIN:-}"
if [ -z "${PYTHON_BIN}" ]; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="python"
  else
    echo "ERROR: Neither python3 nor python was found in PATH." >&2
    exit 1
  fi
fi

suffix="$(date +%s)"
ALICE_USER="sock_alice_${suffix}"
BOB_USER="sock_bob_${suffix}"

cleanup() {
  rm -f /tmp/socket_flow_test_$$.py
}
trap cleanup EXIT

echo "[1/6] Registering users on ${BASE_URL}"
register_user() {
  local username="$1"
  curl -sS -X POST "${BASE_URL}/api/auth/register" \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"${username}\",\"password\":\"${PASSWORD}\",\"public_key\":\"${username}_pub_key\"}" >/dev/null
}

register_user "${ALICE_USER}" || true
register_user "${BOB_USER}" || true

echo "[2/6] Logging in users"
login_payload() {
  local username="$1"
  curl -sS -X POST "${BASE_URL}/api/auth/login" \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"${username}\",\"password\":\"${PASSWORD}\"}"
}

alice_login="$(login_payload "${ALICE_USER}")"
bob_login="$(login_payload "${BOB_USER}")"

ALICE_TOKEN="$(${PYTHON_BIN} - <<'PY' "$alice_login"
import json,sys
print(json.loads(sys.argv[1])["access_token"])
PY
)"
BOB_TOKEN="$(${PYTHON_BIN} - <<'PY' "$bob_login"
import json,sys
print(json.loads(sys.argv[1])["access_token"])
PY
)"

echo "[3/6] Ensuring python socket.io client is available"
${PYTHON_BIN} - <<'PY'
import importlib.util
import subprocess
import sys

if importlib.util.find_spec("socketio") is None:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "python-socketio[client]"])
PY

echo "[4/6] Running live Socket.IO send/receive test"
cat > /tmp/socket_flow_test_$$.py <<'PY'
import json
import sys
import time

import socketio

base_url = sys.argv[1]
alice_token = sys.argv[2]
bob_token = sys.argv[3]
bob_username = sys.argv[4]
timeout_seconds = int(sys.argv[5])
socket_path = sys.argv[6]

alice = socketio.Client(logger=False, engineio_logger=False)
bob = socketio.Client(logger=False, engineio_logger=False)

state = {
    "alice_sent": False,
    "bob_received": False,
    "error": None,
}


@alice.on("message_sent")
def on_message_sent(data):
    state["alice_sent"] = True


@alice.on("message_error")
def on_alice_error(data):
    state["error"] = f"alice error: {data}"


@bob.on("new_message")
def on_new_message(data):
    if data.get("from") and data.get("message") == "socket-flow-test-message":
        state["bob_received"] = True


@bob.on("message_error")
def on_bob_error(data):
    state["error"] = f"bob error: {data}"


bob.connect(
    base_url,
    auth={"token": bob_token},
    transports=["websocket"],
    wait_timeout=timeout_seconds,
    socketio_path=socket_path,
)
alice.connect(
    base_url,
    auth={"token": alice_token},
    transports=["websocket"],
    wait_timeout=timeout_seconds,
    socketio_path=socket_path,
)

alice.emit(
    "send_message",
    {
        "to": bob_username,
        "message": "socket-flow-test-message",
        "encrypted_key": "socket-flow-test-key",
        "type": "text",
    },
)

deadline = time.time() + timeout_seconds
while time.time() < deadline:
    if state["error"]:
        break
    if state["alice_sent"] and state["bob_received"]:
        break
    time.sleep(0.2)

alice.disconnect()
bob.disconnect()

if state["error"]:
    raise SystemExit(state["error"])
if not state["alice_sent"]:
    raise SystemExit("sender did not receive message_sent event")
if not state["bob_received"]:
    raise SystemExit("recipient did not receive new_message event")

print("Socket flow OK")
PY

${PYTHON_BIN} /tmp/socket_flow_test_$$.py "${BASE_URL}" "${ALICE_TOKEN}" "${BOB_TOKEN}" "${BOB_USER}" "${TIMEOUT_SECONDS}" "${SOCKET_PATH}"

echo "[5/6] Validating recipient inbox is empty after live delivery"
inbox_response="$(curl -sS -X GET "${BASE_URL}/api/messages/inbox" -H "Authorization: Bearer ${BOB_TOKEN}")"
${PYTHON_BIN} - <<'PY' "$inbox_response"
import json,sys
payload = json.loads(sys.argv[1])
messages = payload.get("messages", [])
if messages:
    raise SystemExit(f"Expected empty inbox after online delivery, got: {messages}")
print("Inbox check OK")
PY

echo "[6/6] Done"
echo "Created test users: ${ALICE_USER}, ${BOB_USER}"
