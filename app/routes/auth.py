from flask import Blueprint, request, jsonify
from flask_jwt_extended import (
    create_access_token, create_refresh_token,
    jwt_required, get_jwt_identity, get_jwt
)
from werkzeug.security import generate_password_hash, check_password_hash
from app.models.user_model import User
from app.db import db
import redis
import json
import time
from datetime import datetime

# Redis instance
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

auth_bp = Blueprint('auth', __name__)

@auth_bp.route('/register', methods=['POST'])
def register():
    data = request.json
    username = data.get('username')
    password = data.get('password')
    public_key = data.get('public_key')

    if not username or not password or not public_key:
        return jsonify({'error': 'Missing fields'}), 400

    if User.query.filter_by(username=username).first():
        return jsonify({'error': 'Username already exists'}), 400

    hashed_password = generate_password_hash(password)
    user = User(username=username, password_hash=hashed_password, public_key=public_key)
    db.session.add(user)
    db.session.commit()

    return jsonify({'message': 'User registered successfully'}), 201

@auth_bp.route('/login', methods=['POST'])
def login():
    data = request.json
    username = data.get('username')
    password = data.get('password')

    user = User.query.filter_by(username=username).first()
    if not user or not check_password_hash(user.password_hash, password):
        return jsonify({'error': 'Invalid credentials'}), 401

    access_token = create_access_token(identity=username)
    refresh_token = create_refresh_token(identity=username)
    return jsonify({
        'access_token': access_token,
        'refresh_token': refresh_token
    }), 200

@auth_bp.route('/refresh', methods=['POST'])
@jwt_required(refresh=True)
def refresh():
    current_user = get_jwt_identity()
    new_access_token = create_access_token(identity=current_user)
    return jsonify({
        'access_token': new_access_token
    }), 200

@auth_bp.route('/public-key/<username>', methods=['GET'])
@jwt_required()
def public_key(username):
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'error': 'User not found'}), 404
    return jsonify({'public_key': user.public_key})

@auth_bp.route('/contacts/<username>', methods=['GET'])
@jwt_required()
def get_contacts(username):
    user = User.query.filter_by(username=username).first()
    if not user:
        return jsonify({'error': 'User not found'}), 404

    contacts = r.smembers(f"contacts:{username}")
    return jsonify({'contacts': list(contacts)})

@auth_bp.route('/contacts', methods=['POST'])
@jwt_required()
def add_contact():
    data = request.json
    username = get_jwt_identity()
    contact = data.get('contact')

    if not contact:
        return jsonify({'error': 'Missing contact'}), 400

    if not User.query.filter_by(username=contact).first():
        return jsonify({'error': 'Contact user not found'}), 404

    r.sadd(f"contacts:{username}", contact)
    return jsonify({'message': 'Contact added'}), 200

@auth_bp.route('/send', methods=['POST'])
@jwt_required()
def send_message():
    data = request.json
    sender = get_jwt_identity()
    recipient = data.get('to')
    encrypted_message = data.get('message')
    encrypted_key = data.get('encrypted_key')

    if not recipient or not encrypted_message or not encrypted_key:
        return jsonify({'error': 'Missing data'}), 400

    if not User.query.filter_by(username=recipient).first():
        return jsonify({'error': 'Recipient not found'}), 404

    message_data = json.dumps({
        'from': sender,
        'message': encrypted_message,
        'encrypted_key': encrypted_key,
        'timestamp': datetime.utcnow().isoformat()
    })
    r.rpush(f"inbox:{recipient}", message_data)
    return jsonify({'message': 'Message sent'}), 200

@auth_bp.route('/inbox', methods=['GET'])
@jwt_required()
def receive_messages():
    username = get_jwt_identity()
    key = f"inbox:{username}"
    messages = []
    while True:
        msg = r.lpop(key)
        if not msg:
            break
        messages.append(json.loads(msg))
    return jsonify({'messages': messages})
