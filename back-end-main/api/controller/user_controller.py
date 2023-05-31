from flask import jsonify, request

from controller import app
from service.authenticate import jwt_required
from service.user_service import (
    delete_user,
    get_user,
    get_users,
    login_user,
    register_user,
)


@app.route("/register", methods=["POST"])
def register():
    try:
        username = request.json.get("name")
        user_email = request.json.get("email")
        user_password = request.json.get("password")

        status_code, response_message = register_user(
            username=username, user_email=user_email, user_password=user_password
        )

        return jsonify({"message": response_message}), status_code
    except:
        return jsonify({"message": "user_invalid_data"}), 400


@app.route("/login", methods=["POST"])
def login():
    try:
        user_email = request.json.get("email")
        user_password = request.json.get("password")

        user, token, status_code, message_response = login_user(
            user_email=user_email, user_password=user_password
        )

        if (not user) or (not token):
            return jsonify({"message": message_response}), status_code

        return jsonify(
            {"message": message_response, "is_admin": user.is_admin, "token": token}
        )
    except:
        return jsonify({"message": "user_invalid_data"}), 400


@app.route("/getUser", methods=["GET"])
@jwt_required
def getUser(current_user):
    try:
        user_dict, status_code = get_user(current_user=current_user)

        return jsonify(user_dict), status_code
    except:
        return jsonify({"message": "user_invalid_data"}), 400


@app.route("/getUsers", methods=["GET"])
@jwt_required
def getUsers(current_user):
    try:
        users_dictionary, status_code, response_message = get_users(
            current_user=current_user
        )

        if not users_dictionary:
            return jsonify({"message": response_message}), status_code

        return jsonify(users_dictionary), status_code
    except:
        return jsonify({"message": "users_invalid_data"}), 400


@app.route("/deleteUser", methods=["DELETE"])
@jwt_required
def deleteUser(current_user):
    try:
        user_email = request.json.get("email")

        status_code, response_message = delete_user(
            current_user=current_user, user_email=user_email
        )

        return jsonify({"message": response_message}), status_code
    except:
        return jsonify({"message": "user_invalid_data"}), 400
