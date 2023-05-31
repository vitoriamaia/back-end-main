from flask_login import UserMixin
from werkzeug.security import check_password_hash, generate_password_hash

from controller import db, ma


class User(db.Model, UserMixin):
    __tablename__ = "users"
    id = db.Column(db.Integer, autoincrement=True, primary_key=True)
    name = db.Column(db.String(200), nullable=False)
    email = db.Column(db.String(300), nullable=False)
    password = db.Column(db.String(200), nullable=False)
    is_admin = db.Column(db.Integer, nullable=False, server_default="0")

    def __init__(self, name, email, password, is_admin):
        self.name = name
        self.email = email
        self.password = generate_password_hash(password)
        self.is_admin = is_admin

    def verify_password(self, pwd):
        return check_password_hash(self.password, pwd)

    def __repr__(self):
        return f"<User : {self.name}>"


class UserSchema(ma.Schema):
    class Meta:
        fields = ("id", "name", "email", "password", "is_admin")


user_share_schema = UserSchema()
users_share_schema = UserSchema(many=True)
