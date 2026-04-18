from datetime import datetime

from app.db import db


class CrashLog(db.Model):
    __tablename__ = "crash_logs"

    id = db.Column(db.Integer, primary_key=True)
    event_id = db.Column(db.String(64), nullable=False, unique=True, index=True)
    platform = db.Column(db.String(16), nullable=False, default="android", index=True)
    app_version = db.Column(db.String(64), nullable=False, index=True)
    app_version_code = db.Column(db.Integer, nullable=True, index=True)
    thread_name = db.Column(db.String(120), nullable=True)
    exception_type = db.Column(db.String(255), nullable=False)
    exception_message = db.Column(db.String(2048), nullable=True)
    stack_trace = db.Column(db.Text, nullable=False)
    occurred_at = db.Column(db.DateTime, nullable=False, index=True)
    received_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    user_id = db.Column(db.Integer, nullable=True, index=True)
    username_snapshot = db.Column(db.String(80), nullable=True)
    device_model = db.Column(db.String(120), nullable=True)
    device_manufacturer = db.Column(db.String(120), nullable=True)
    os_version = db.Column(db.String(120), nullable=True)
    sdk_int = db.Column(db.Integer, nullable=True)
    build_type = db.Column(db.String(40), nullable=True)


class CrashMappingFile(db.Model):
    __tablename__ = "crash_mapping_files"

    id = db.Column(db.Integer, primary_key=True)
    platform = db.Column(db.String(16), nullable=False, default="android", index=True)
    app_version = db.Column(db.String(64), nullable=False, index=True)
    app_version_code = db.Column(db.Integer, nullable=True, index=True)
    original_filename = db.Column(db.String(255), nullable=True)
    mapping_text = db.Column(db.Text, nullable=False)
    uploaded_by_admin_id = db.Column(db.Integer, nullable=True, index=True)
    uploaded_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)

    __table_args__ = (
        db.UniqueConstraint("platform", "app_version", name="uq_crash_mapping_platform_version"),
    )
