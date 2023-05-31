import datetime

import jwt

from controller import app, db
from model.user_model import User, user_share_schema, users_share_schema


def register_user(
    username: str, user_email: str, user_password: str
) -> tuple[int, str]:
    """
    This function registers a user.

    Parameters:
    ----------
    username : str
        Username.

    user_email : str
        User email.

    user_password : str
        User password.

    Returns:
    -------
    tuple[int, str]
        (status code, response message).
    """

    user = User.query.filter_by(email=user_email).first()

    if user:
        return (409, "user_already_registered")

    user = User(name=username, email=user_email, password=user_password, is_admin=False)

    db.session.add(user)
    db.session.commit()

    return (201, "user_registered")


def login_user(
    user_email: str, user_password: str
) -> tuple[None, None, int, str] | tuple[User, str, int, str]:
    """
    This function login a registered user.

    Parameters:
    ----------
    user_email : str
        User email.

    user_password : str
        User password.

    Returns:
    -------
    tuple[None, None, int, str] | tuple[User, str, int, str]
        If an error occurs, the return will be: (None, None, status code, response message).
        Else the return will be: (user object, token, status code, response message).
    """

    user = User.query.filter_by(email=user_email).first()

    if not user or not user.verify_password(user_password):
        return (None, None, 403, "user_incorrect_data")

    payload = {
        "id": user.id,
        "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=2880),
    }

    token = jwt.encode(payload, app.config["SECRET_KEY"])

    return (user, token, 200, "user_logged")


def get_user(current_user: User) -> tuple[dict, int]:
    """
    This function gets user informations.

    Parameters:
    ----------
    current_user : User
        Object representing the current user.

    Returns:
    -------
    tuple[dict, int]
        (user dictionary, status code).
    """

    return (user_share_schema.dump(current_user), 200)


def get_users(current_user: User) -> tuple[dict, int, str]:
    """
    This function gets informations all user.
    Parameters:
    ----------
    current_user : User
        Object representing the current user.

    Returns:
    -------
    tuple[dict, int, str]
        (users dictionary, status code, response message).
    """

    if current_user.is_admin != 1:
        return (None, 403, "required_administrator_privileges")

    return (users_share_schema.dump(User.query.all()), 200, None)


def delete_user(current_user: User, user_email: str) -> tuple[int, str]:
    """
    This function deletes a registered user.

    Parameters:
    ----------
    current_user : User
        Object representing the current user.

    user_email : str
        User email.

    Returns:
    -------
    tuple[int, str]
        (status code, response message).
    """

    if current_user.is_admin != 1:
        return (403, "required_administrator_privileges")

    user = User.query.filter_by(email=user_email).first()

    if not user:
        return (404, "user_not_found")

    db.session.delete(user)
    db.session.commit()

    user_deleted = user_share_schema.dump(
        User.query.filter_by(email=user_email).first()
    )

    if user_deleted:
        return (500, "user_not_deleted")

    return (200, "user_deleted")
