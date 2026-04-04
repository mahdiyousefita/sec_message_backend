from flask_marshmallow import Marshmallow
from flask_socketio import SocketIO

ma = Marshmallow()


def _resolve_socketio_async_mode():
    try:
        import eventlet  # noqa: F401

        return "eventlet"
    except Exception:
        # Keep local/dev test runs working even when eventlet is not installed.
        return "threading"


socketio = SocketIO(async_mode=_resolve_socketio_async_mode())
