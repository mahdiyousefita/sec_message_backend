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
from app.routes.crash_routes import crash_bp
from app.services import async_task_service, report_service, password_security

import app.models.activity_notification_model  # noqa: F401 – register model with SQLAlchemy
import app.models.about_us_model  # noqa: F401 – register model with SQLAlchemy
import app.models.app_update_model  # noqa: F401 – register model with SQLAlchemy
import app.models.block_model  # noqa: F401 – register model with SQLAlchemy
import app.models.chat_message_model  # noqa: F401 – register model with SQLAlchemy
import app.models.crash_log_model  # noqa: F401 – register model with SQLAlchemy
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


def _ensure_post_quote_schema():
    inspector = inspect(db.engine)
    if not inspector.has_table("posts"):
        return

    column_names = {
        column["name"]
        for column in inspector.get_columns("posts")
    }
    if "quoted_post_id" not in column_names:
        db.session.execute(
            text(
                "ALTER TABLE posts "
                "ADD COLUMN quoted_post_id INTEGER"
            )
        )

    db.session.execute(
        text(
            "CREATE INDEX IF NOT EXISTS ix_posts_quoted_post_id "
            "ON posts (quoted_post_id)"
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


def _ensure_about_us_schema():
    inspector = inspect(db.engine)
    if not inspector.has_table("about_us_team_members"):
        return

    column_names = {
        column["name"]
        for column in inspector.get_columns("about_us_team_members")
    }
    if "display_name" not in column_names:
        db.session.execute(
            text(
                "ALTER TABLE about_us_team_members "
                "ADD COLUMN display_name VARCHAR(120)"
            )
        )
        db.session.commit()


def _ensure_private_message_sender_cipher_schema():
    inspector = inspect(db.engine)
    if not inspector.has_table("private_messages"):
        return

    column_names = {
        column["name"]
        for column in inspector.get_columns("private_messages")
    }
    migration_sql = []
    if "sender_encrypted_message" not in column_names:
        migration_sql.append(
            "ALTER TABLE private_messages "
            "ADD COLUMN sender_encrypted_message TEXT"
        )
    if "sender_encrypted_key" not in column_names:
        migration_sql.append(
            "ALTER TABLE private_messages "
            "ADD COLUMN sender_encrypted_key TEXT"
        )

    if migration_sql:
        for sql in migration_sql:
            db.session.execute(text(sql))
        db.session.commit()


def _ensure_message_idempotency_schema():
    inspector = inspect(db.engine)

    if inspector.has_table("private_messages"):
        private_columns = {
            column["name"]
            for column in inspector.get_columns("private_messages")
        }
        if "client_message_id" not in private_columns:
            db.session.execute(
                text(
                    "ALTER TABLE private_messages "
                    "ADD COLUMN client_message_id VARCHAR(128)"
                )
            )

        db.session.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS "
                "ux_private_messages_sender_recipient_client_message_id "
                "ON private_messages (sender_username, recipient_username, client_message_id)"
            )
        )

    if inspector.has_table("group_messages"):
        group_columns = {
            column["name"]
            for column in inspector.get_columns("group_messages")
        }
        if "client_message_id" not in group_columns:
            db.session.execute(
                text(
                    "ALTER TABLE group_messages "
                    "ADD COLUMN client_message_id VARCHAR(128)"
                )
            )

        db.session.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS "
                "ux_group_messages_sender_group_client_message_id "
                "ON group_messages (sender_username, group_id, client_message_id)"
            )
        )

    db.session.commit()


def _ensure_group_recipient_seen_schema():
    inspector = inspect(db.engine)
    if not inspector.has_table("group_message_recipients"):
        return

    column_names = {
        column["name"]
        for column in inspector.get_columns("group_message_recipients")
    }

    migration_sql = []
    if "seen_at" not in column_names:
        migration_sql.append(
            "ALTER TABLE group_message_recipients "
            "ADD COLUMN seen_at TIMESTAMP"
        )

    for sql in migration_sql:
        db.session.execute(text(sql))

    db.session.execute(
        text(
            "CREATE INDEX IF NOT EXISTS ix_group_message_recipient_seen "
            "ON group_message_recipients (recipient_username, group_id, seen_at)"
        )
    )
    db.session.commit()


def _ensure_group_key_fanout_schema():
    inspector = inspect(db.engine)
    if not inspector.has_table("group_messages"):
        return

    group_message_columns = {
        column["name"]
        for column in inspector.get_columns("group_messages")
    }
    if "group_key_ref" not in group_message_columns:
        db.session.execute(
            text(
                "ALTER TABLE group_messages "
                "ADD COLUMN group_key_ref VARCHAR(128)"
            )
        )
    if "sender_encrypted_key" not in group_message_columns:
        db.session.execute(
            text(
                "ALTER TABLE group_messages "
                "ADD COLUMN sender_encrypted_key TEXT"
            )
        )

    db.session.execute(
        text(
            "CREATE INDEX IF NOT EXISTS ix_group_messages_group_key_ref "
            "ON group_messages (group_key_ref)"
        )
    )

    if inspector.has_table("group_message_recipients"):
        recipient_columns = {
            column["name"]
            for column in inspector.get_columns("group_message_recipients")
        }
        if "encrypted_key" not in recipient_columns:
            db.session.execute(
                text(
                    "ALTER TABLE group_message_recipients "
                    "ADD COLUMN encrypted_key TEXT"
                )
            )

    db.session.execute(
        text(
            "CREATE TABLE IF NOT EXISTS group_message_key_recipients ("
            "id INTEGER PRIMARY KEY, "
            "group_id INTEGER NOT NULL, "
            "sender_username VARCHAR(80) NOT NULL, "
            "group_key_ref VARCHAR(128) NOT NULL, "
            "recipient_username VARCHAR(80) NOT NULL, "
            "encrypted_key TEXT NOT NULL, "
            "created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"
            ")"
        )
    )
    db.session.execute(
        text(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_group_message_key_recipient "
            "ON group_message_key_recipients ("
            "group_id, sender_username, group_key_ref, recipient_username)"
        )
    )
    db.session.execute(
        text(
            "CREATE INDEX IF NOT EXISTS ix_group_message_key_lookup "
            "ON group_message_key_recipients (group_id, sender_username, group_key_ref)"
        )
    )
    db.session.commit()


def _ensure_message_user_delete_schema():
    db.session.execute(
        text(
            "CREATE TABLE IF NOT EXISTS private_message_user_deletes ("
            "id INTEGER PRIMARY KEY, "
            "message_id VARCHAR(64) NOT NULL, "
            "username VARCHAR(80) NOT NULL, "
            "deleted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"
            ")"
        )
    )
    db.session.execute(
        text(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_private_message_user_delete "
            "ON private_message_user_deletes (message_id, username)"
        )
    )
    db.session.execute(
        text(
            "CREATE INDEX IF NOT EXISTS ix_private_message_user_delete_lookup "
            "ON private_message_user_deletes (username, message_id)"
        )
    )

    db.session.execute(
        text(
            "CREATE TABLE IF NOT EXISTS group_message_user_deletes ("
            "id INTEGER PRIMARY KEY, "
            "message_id VARCHAR(64) NOT NULL, "
            "group_id INTEGER NOT NULL, "
            "username VARCHAR(80) NOT NULL, "
            "deleted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"
            ")"
        )
    )
    db.session.execute(
        text(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_group_message_user_delete "
            "ON group_message_user_deletes (message_id, username)"
        )
    )
    db.session.execute(
        text(
            "CREATE INDEX IF NOT EXISTS ix_group_message_user_delete_lookup "
            "ON group_message_user_deletes (group_id, username, message_id)"
        )
    )
    db.session.commit()


def _ensure_crash_log_schema():
    inspector = inspect(db.engine)
    if not inspector.has_table("crash_logs"):
        return

    column_names = {
        column["name"]
        for column in inspector.get_columns("crash_logs")
    }
    if "crash_signature" not in column_names:
        db.session.execute(
            text(
                "ALTER TABLE crash_logs "
                "ADD COLUMN crash_signature VARCHAR(64)"
            )
        )
    if "occurrence_count" not in column_names:
        db.session.execute(
            text(
                "ALTER TABLE crash_logs "
                "ADD COLUMN occurrence_count INTEGER NOT NULL DEFAULT 1"
            )
        )
    if "affected_users_json" not in column_names:
        db.session.execute(
            text(
                "ALTER TABLE crash_logs "
                "ADD COLUMN affected_users_json TEXT"
            )
        )

    db.session.execute(
        text(
            "CREATE INDEX IF NOT EXISTS ix_crash_logs_crash_signature "
            "ON crash_logs (crash_signature)"
        )
    )
    db.session.execute(
        text(
            "CREATE INDEX IF NOT EXISTS ix_crash_logs_occurrence_count "
            "ON crash_logs (occurrence_count)"
        )
    )
    db.session.execute(
        text(
            "CREATE INDEX IF NOT EXISTS ix_resolved_crash_signatures_signature "
            "ON resolved_crash_signatures (signature)"
        )
    )
    db.session.execute(
        text(
            "CREATE INDEX IF NOT EXISTS ix_crash_event_ids_event_id "
            "ON crash_event_ids (event_id)"
        )
    )
    db.session.execute(
        text(
            "CREATE INDEX IF NOT EXISTS ix_crash_event_ids_crash_log_id "
            "ON crash_event_ids (crash_log_id)"
        )
    )
    db.session.commit()


def _ensure_user_created_at_schema():
    inspector = inspect(db.engine)
    if not inspector.has_table("users"):
        return

    column_names = {
        column["name"]
        for column in inspector.get_columns("users")
    }
    if "created_at" not in column_names:
        db.session.execute(
            text(
                "ALTER TABLE users "
                "ADD COLUMN created_at TIMESTAMP"
            )
        )

    db.session.execute(
        text(
            "UPDATE users SET created_at = CURRENT_TIMESTAMP "
            "WHERE created_at IS NULL"
        )
    )
    db.session.commit()


def _ensure_user_badge_schema():
    inspector = inspect(db.engine)
    if not inspector.has_table("users"):
        return

    column_names = {
        column["name"]
        for column in inspector.get_columns("users")
    }
    if "badge" not in column_names:
        db.session.execute(
            text(
                "ALTER TABLE users "
                "ADD COLUMN badge VARCHAR(64)"
            )
        )

    db.session.execute(
        text(
            "CREATE INDEX IF NOT EXISTS ix_users_badge "
            "ON users (badge)"
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
    app_env = str(app.config.get("APP_ENV", "development")).strip().lower()
    inline_fallback = bool(app.config.get("ASYNC_TASK_INLINE_FALLBACK", True))
    min_workers = int(app.config.get("ASYNC_TASK_MIN_WORKER_COUNT", 1) or 1)

    if app.config.get("ASYNC_TASKS_ENABLED", False):
        app.logger.info(
            "Async task queue enabled (queue=%s app_env=%s inline_fallback=%s min_workers=%s). "
            "Run `python run_async_worker.py` in production.",
            app.config.get("ASYNC_TASK_QUEUE_NAME"),
            app_env,
            inline_fallback,
            min_workers,
        )
        if app_env in {"prod", "production"} and inline_fallback:
            app.logger.warning(
                "APP_ENV=%s with ASYNC_TASK_INLINE_FALLBACK=true. "
                "Group message side effects still require worker-backed enqueue and will not fallback inline.",
                app_env,
            )
        return
    app.logger.info(
        "Async task queue disabled (app_env=%s); side effects run inline where enabled.",
        app_env,
    )


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

    app.config["MAX_CONTENT_LENGTH"] = 100 * 1024 * 1024  # 100 MB hard cap
    _warn_on_database_fallback(app)
    _log_async_task_mode(app)
    if app.config.get("ASYNC_TASK_SKIP_STARTUP_WORKER_CHECK", False):
        app.logger.info("Skipping async worker startup capacity check for this process.")
    else:
        with app.app_context():
            workers_ok = async_task_service.verify_worker_capacity_for_startup(
                source="create_app",
            )
        if not workers_ok:
            raise RuntimeError(
                "Async worker capacity check failed. "
                "Start async workers or set ASYNC_TASK_WORKER_STARTUP_STRICT=false."
            )

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
    with app.app_context():
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
    app.register_blueprint(crash_bp, url_prefix="/api")
    app.register_blueprint(block_bp, url_prefix="/api")
    app.register_blueprint(playlist_bp, url_prefix="/api")
    app.register_blueprint(admin_bp, url_prefix="/admin")
    app.register_blueprint(group_bp, url_prefix="/api/groups")

    with app.app_context():
        db.create_all()
        _ensure_post_visibility_schema()
        _ensure_post_quote_schema()
        _ensure_media_schema()
        _ensure_performance_indexes()
        _ensure_app_update_schema()
        _ensure_about_us_schema()
        _ensure_private_message_sender_cipher_schema()
        _ensure_message_idempotency_schema()
        _ensure_group_recipient_seen_schema()
        _ensure_group_key_fanout_schema()
        _ensure_message_user_delete_schema()
        _ensure_crash_log_schema()
        _ensure_user_created_at_schema()
        _ensure_user_badge_schema()
        migrated_passwords = password_security.migrate_plaintext_passwords()
        if migrated_passwords:
            app.logger.info(
                "Migrated %s legacy plaintext password records to Argon2id hashes.",
                migrated_passwords,
            )

    if os.getenv("FLASK_RUN_FROM_CLI", "").strip().lower() in {"1", "true"}:
        app.logger.info("Using Flask CLI runtime. For production prefer Gunicorn.")
    _start_moderation_cleanup_worker(app)

    # error handler
    @app.errorhandler(HTTPException)
    def handle_http_exception(e):
        return jsonify({"error": e.description}), e.code

    return app
