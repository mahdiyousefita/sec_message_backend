"""
One-time migration: create the ``admin_users`` table and
promote the MahdiAdmin user to admin.

Run from the project root:
    source venv/bin/activate
    python migrate_add_admin.py
"""

from app import create_app
from app.db import db
from app.models.admin_model import AdminUser
from app.models.user_model import User


def migrate():
    app = create_app()

    with app.app_context():
        db.create_all()
        print("[*] admin_users table ready.")

        user = User.query.filter_by(username="MahdiAdmin").first()
        if not user:
            print("[!] User 'MahdiAdmin' not found – you can promote them later from the panel.")
            return

        existing = AdminUser.query.filter_by(user_id=user.id).first()
        if existing:
            print("[*] MahdiAdmin is already an admin.")
            return

        db.session.add(AdminUser(user_id=user.id))
        db.session.commit()
        print("[+] MahdiAdmin promoted to admin.")

    print("[✓] Migration complete.")


if __name__ == "__main__":
    migrate()
