from flask import Flask, jsonify
from flask_jwt_extended import JWTManager

from app.extensions.extensions import ma

from app.config import Config
from app.db import db
from werkzeug.exceptions import HTTPException
from app.routes.post_routes import post_bp
from app.routes.comment_routes import comment_bp
from app.routes.vote_routes import vote_bp
from app.routes.auth_routes import auth_bp
from app.routes.contact_routes import contact_bp
from app.routes.message_routes import message_bp



def create_app():
    app = Flask(__name__)


    # app.config["JWT_SECRET_KEY"] = "change-this-secret"
    # app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///messenger.db"
    # app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False


    app.config.from_object(Config)

    db.init_app(app)
    ma.init_app(app)
    jwt = JWTManager(app)

    app.register_blueprint(auth_bp, url_prefix="/api/auth")
    app.register_blueprint(contact_bp, url_prefix="/api/contacts")
    app.register_blueprint(message_bp, url_prefix="/api/messages")
    app.register_blueprint(post_bp, url_prefix="/api")
    app.register_blueprint(comment_bp, url_prefix="/api")
    app.register_blueprint(vote_bp, url_prefix="/api")

    with app.app_context():
        db.create_all()

    # error handler
    @app.errorhandler(HTTPException)
    def handle_http_exception(e):
        return jsonify({"error": e.description}), e.code
    return app
