from flask_login import UserMixin

from controller import db, ma


class DatabaseKey(db.Model, UserMixin):
    __tablename__ = "databases_keys"

    id = db.Column(db.Integer, nullable=False, autoincrement=True, primary_key=True)
    id_db = db.Column(db.Integer, db.ForeignKey("databases.id"), nullable=False)
    public_key = db.Column(db.Text, nullable=False)
    private_key = db.Column(db.Text, nullable=False)

    def __init__(self, id_db, public_key, private_key):
        self.id_db = id_db
        self.public_key = public_key
        self.private_key = private_key

    def __repr__(self):
        return f"<id_db : {self.id_db}, public_key: {self.public_key, }private_key : {self.private_key}>"


class DatabaseKeySchema(ma.Schema):
    class Meta:
        fields = ("id", "id_db", "public_key", "private_key")


database_key_share_schema = DatabaseKeySchema()
databases_keys_share_schema = DatabaseKeySchema(many=True)
