from functools import wraps

import jwt
from flask import current_app, jsonify, request
from model.user_model import User


def jwt_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):

        token = None

        if "authorization" in request.headers:
            token = request.headers["authorization"]

        if not token:
            return jsonify({"message": "token_not_found"}), 404

        if not "Bearer" in token:
            return jsonify({"message": "token_invalid"}), 400

        try:
            token_pure = token.replace("Bearer ", "")
            decoded = jwt.decode(
                token_pure, current_app.config["SECRET_KEY"], algorithms=["HS256"]
            )
            current_user = User.query.get(decoded["id"])
        except:
            return jsonify({"message": "token_expired"}), 403
        return f(current_user=current_user, *args, **kwargs)

    return wrapper
