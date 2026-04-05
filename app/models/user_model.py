from app.db import db


class User(db.Model):
    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)
    public_key = db.Column(db.Text, nullable=False)
    is_suspended = db.Column(db.Boolean, nullable=False, default=False, index=True)
    suspended_at = db.Column(db.DateTime, nullable=True, index=True)
    purge_after = db.Column(db.DateTime, nullable=True, index=True)
    suspension_reason = db.Column(db.String(32), nullable=True)
    suspended_by_report_id = db.Column(db.Integer, nullable=True, index=True)

    def to_dict(self):
        return {
            'username': self.username,
            'public_key': self.public_key
        }
