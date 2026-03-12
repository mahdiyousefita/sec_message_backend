from flask_marshmallow import Marshmallow
from flask_socketio import SocketIO

ma = Marshmallow()
socketio = SocketIO(async_mode="threading")
