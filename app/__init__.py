from flask import Flask, jsonify
from flask_cors import CORS
from flask_jwt_extended import JWTManager

from app.extensions.extensions import ma, socketio

from app.config import Config
from app.db import db
from werkzeug.exceptions import HTTPException
from app.socket_events import register_socket_events

from app.routes.main_routes import main_bp
from app.routes.post_routes import post_bp
from app.routes.comment_routes import comment_bp
from app.routes.vote_routes import vote_bp
from app.routes.auth_routes import auth_bp
from app.routes.contact_routes import contact_bp
from app.routes.message_routes import message_bp
from app.routes.profile_routes import profile_bp
from app.routes.notification_routes import notification_bp
from app.routes.activity_notification_routes import activity_notification_bp
from app.routes.follow_routes import follow_bp
from app.routes.search_routes import search_bp
from app.routes.admin_routes import admin_bp
from app.routes.group_routes import group_bp

import app.models.activity_notification_model  # noqa: F401 – register model with SQLAlchemy
import app.models.group_model  # noqa: F401 – register model with SQLAlchemy


def create_app():
    app = Flask(__name__,
                template_folder='templates',
                static_folder='static')

    app.config.from_object(Config)

    app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB hard cap

    app.config["SESSION_COOKIE_SAMESITE"] = "None"
    app.config["SESSION_COOKIE_SECURE"] = True
    app.config["JWT_COOKIE_SAMESITE"] = "None"
    app.config["JWT_COOKIE_SECURE"] = True

    CORS(
        app,
        resources={r"/*": {"origins": Config.CORS_ALLOWED_ORIGINS}},
        supports_credentials=True,
    )

    db.init_app(app)
    ma.init_app(app)
    socketio.init_app(
        app,
        cors_allowed_origins=Config.SOCKETIO_CORS_ALLOWED_ORIGINS,
        message_queue=Config.SOCKETIO_MESSAGE_QUEUE,
        ping_timeout=Config.SOCKETIO_PING_TIMEOUT,
        ping_interval=Config.SOCKETIO_PING_INTERVAL,
        logger=Config.SOCKETIO_LOGGER,
        engineio_logger=Config.SOCKETIO_ENGINEIO_LOGGER,
    )
    jwt = JWTManager(app)
    register_socket_events()

    app.register_blueprint(main_bp, url_prefix="/")
    app.register_blueprint(auth_bp, url_prefix="/api/auth")
    app.register_blueprint(contact_bp, url_prefix="/api/contacts")
    app.register_blueprint(message_bp, url_prefix="/api/messages")
    app.register_blueprint(post_bp, url_prefix="/api")
    app.register_blueprint(comment_bp, url_prefix="/api")
    app.register_blueprint(vote_bp, url_prefix="/api")
    app.register_blueprint(notification_bp, url_prefix="/api/notifications")
    app.register_blueprint(activity_notification_bp, url_prefix="/api/activity-notifications")
    app.register_blueprint(profile_bp, url_prefix="/api")
    app.register_blueprint(follow_bp, url_prefix="/api")
    app.register_blueprint(search_bp, url_prefix="/api")
    app.register_blueprint(admin_bp, url_prefix="/admin")
    app.register_blueprint(group_bp, url_prefix="/api/groups")

    with app.app_context():
        db.create_all()

    # error handler
    @app.errorhandler(HTTPException)
    def handle_http_exception(e):
        return jsonify({"error": e.description}), e.code

    return app
