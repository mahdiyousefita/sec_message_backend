import os
import threading
import time

from flask import Flask, jsonify
from flask_jwt_extended import JWTManager
from sqlalchemy import inspect, text

from app.extensions.extensions import ma, socketio

from app.config import Config
from app.db import db
from app.performance_indexes import ensure_performance_indexes
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
from app.routes.report_routes import report_bp
from app.routes.block_routes import block_bp
from app.routes.playlist_routes import playlist_bp
from app.services import async_task_service, report_service

import app.models.activity_notification_model  # noqa: F401 – register model with SQLAlchemy
import app.models.app_update_model  # noqa: F401 – register model with SQLAlchemy
import app.models.block_model  # noqa: F401 – register model with SQLAlchemy
import app.models.group_model  # noqa: F401 – register model with SQLAlchemy
import app.models.pending_registration_model  # noqa: F401 – register model with SQLAlchemy
import app.models.playlist_track_model  # noqa: F401 – register model with SQLAlchemy
import app.models.report_model  # noqa: F401 – register model with SQLAlchemy

_cleanup_worker_started = False
_cleanup_worker_lock = threading.Lock()


def _ensure_post_visibility_schema():
    inspector = inspect(db.engine)
    if not inspector.has_table("posts"):
        return

    column_names = {
        column["name"]
        for column in inspector.get_columns("posts")
    }
    if "followers_only" not in column_names:
        db.session.execute(
            text(
                "ALTER TABLE posts "
                "ADD COLUMN followers_only BOOLEAN NOT NULL DEFAULT FALSE"
            )
        )

    db.session.execute(
        text(
            "CREATE INDEX IF NOT EXISTS ix_posts_followers_only "
            "ON posts (followers_only)"
        )
    )

    db.session.commit()


def _ensure_media_schema():
    inspector = inspect(db.engine)
    if not inspector.has_table("media"):
        return

    column_names = {
        column["name"]
        for column in inspector.get_columns("media")
    }
    migration_sql = []
    if "display_name" not in column_names:
        migration_sql.append(
            "ALTER TABLE media ADD COLUMN display_name VARCHAR(255)"
        )
    if "title" not in column_names:
        migration_sql.append(
            "ALTER TABLE media ADD COLUMN title VARCHAR(255)"
        )
    if "artist" not in column_names:
        migration_sql.append(
            "ALTER TABLE media ADD COLUMN artist VARCHAR(255)"
        )

    if migration_sql:
        for sql in migration_sql:
            db.session.execute(text(sql))
        db.session.commit()


def _ensure_performance_indexes():
    ensure_performance_indexes(db.session, db.engine)


def _ensure_app_update_schema():
    inspector = inspect(db.engine)
    if not inspector.has_table("app_update_configs"):
        return

    column_names = {
        column["name"]
        for column in inspector.get_columns("app_update_configs")
    }

    if "latest_version" not in column_names:
        db.session.execute(
            text(
                "ALTER TABLE app_update_configs "
                "ADD COLUMN latest_version VARCHAR(32)"
            )
        )
    if "force_update_below" not in column_names:
        db.session.execute(
            text(
                "ALTER TABLE app_update_configs "
                "ADD COLUMN force_update_below VARCHAR(32)"
            )
        )
    if "optional_update_below" not in column_names:
        db.session.execute(
            text(
                "ALTER TABLE app_update_configs "
                "ADD COLUMN optional_update_below VARCHAR(32)"
            )
        )
    if "force_title" not in column_names:
        db.session.execute(
            text(
                "ALTER TABLE app_update_configs "
                "ADD COLUMN force_title VARCHAR(120)"
            )
        )
    if "force_message" not in column_names:
        db.session.execute(
            text(
                "ALTER TABLE app_update_configs "
                "ADD COLUMN force_message VARCHAR(255)"
            )
        )
    if "optional_title" not in column_names:
        db.session.execute(
            text(
                "ALTER TABLE app_update_configs "
                "ADD COLUMN optional_title VARCHAR(120)"
            )
        )
    if "optional_message" not in column_names:
        db.session.execute(
            text(
                "ALTER TABLE app_update_configs "
                "ADD COLUMN optional_message VARCHAR(255)"
            )
        )
    db.session.commit()


def _warn_on_database_fallback(app: Flask):
    if Config.DATABASE_URL_WAS_EXPLICIT:
        return
    app.logger.warning(
        "DATABASE_URL is not set; using sqlite:///messenger.db. "
        "Use a managed DB in production and run with Gunicorn."
    )


def _log_async_task_mode(app: Flask):
    if app.config.get("ASYNC_TASKS_ENABLED", False):
        app.logger.info(
            "Async task queue enabled (queue=%s). "
            "Run `python run_async_worker.py` in production.",
            app.config.get("ASYNC_TASK_QUEUE_NAME"),
        )
        return
    app.logger.info("Async task queue disabled; side effects run inline.")


def _start_moderation_cleanup_worker(app: Flask):
    global _cleanup_worker_started

    enabled = bool(app.config.get("MODERATION_CLEANUP_BACKGROUND_ENABLED", True))
    if not enabled:
        app.logger.info(
            "Moderation cleanup in-process worker disabled. "
            "Run `python run_cleanup_worker.py --once` from cron for external scheduling."
        )
        return

    with _cleanup_worker_lock:
        if _cleanup_worker_started:
            return
        _cleanup_worker_started = True

    interval = max(
        int(app.config.get("MODERATION_CLEANUP_INTERVAL_SECONDS", 300)),
        10,
    )
    batch_size = max(
        int(app.config.get("MODERATION_CLEANUP_BATCH_SIZE", 100)),
        1,
    )

    app_ref = app

    def _worker():
        app_ref.logger.info(
            "Moderation cleanup background worker started (interval=%ss, batch_size=%s)",
            interval,
            batch_size,
        )
        # Delay first run to keep startup and tests fast.
        while True:
            time.sleep(interval)
            try:
                with app_ref.app_context():
                    enqueued = async_task_service.enqueue_cleanup_task(
                        force=True,
                        batch_size=batch_size,
                        source="inprocess_cleanup_scheduler",
                    )
                    if not enqueued:
                        report_service.run_scheduled_cleanup_with_metrics(
                            force=True,
                            batch_size=batch_size,
                        )
            except Exception:
                app_ref.logger.exception("Moderation cleanup worker failed")

    thread = threading.Thread(
        target=_worker,
        name="moderation-cleanup-worker",
        daemon=True,
    )
    thread.start()


def create_app():
    app = Flask(__name__,
                template_folder='templates',
                static_folder='static')

    app.config.from_object(Config)

    app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB hard cap
    _warn_on_database_fallback(app)
    _log_async_task_mode(app)

    db.init_app(app)
    ma.init_app(app)
    socketio.init_app(
        app,
        # Keep reverse-proxy behavior by default; local .env can override.
        cors_allowed_origins=app.config["SOCKETIO_CORS_ALLOWED_ORIGINS"],
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
    app.register_blueprint(report_bp, url_prefix="/api")
    app.register_blueprint(block_bp, url_prefix="/api")
    app.register_blueprint(playlist_bp, url_prefix="/api")
    app.register_blueprint(admin_bp, url_prefix="/admin")
    app.register_blueprint(group_bp, url_prefix="/api/groups")

    with app.app_context():
        db.create_all()
        _ensure_post_visibility_schema()
        _ensure_media_schema()
        _ensure_performance_indexes()
        _ensure_app_update_schema()

    if os.getenv("FLASK_RUN_FROM_CLI", "").strip().lower() in {"1", "true"}:
        app.logger.info("Using Flask CLI runtime. For production prefer Gunicorn.")
    _start_moderation_cleanup_worker(app)

    # error handler
    @app.errorhandler(HTTPException)
    def handle_http_exception(e):
        return jsonify({"error": e.description}), e.code

    return app
