from flask_marshmallow import Marshmallow
from flask_socketio import SocketIO

ma = Marshmallow()
socketio = SocketIO(cors_allowed_origins="*", async_mode="threading")
