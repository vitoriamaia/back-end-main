from flask_login import UserMixin

from controller import db, ma


class ValidDatabase(db.Model, UserMixin):
    __tablename__ = "valid_databases"

    id = db.Column(db.Integer, nullable=False, autoincrement=True, primary_key=True)
    name = db.Column(db.String(100), nullable=False)

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f"<ValidDatabase : {self.name}>"


class ValidDatabaseSchema(ma.Schema):
    class Meta:
        fields = ("id", "name")


valid_database_share_schema = ValidDatabaseSchema()
valid_databases_share_schema = ValidDatabaseSchema(many=True)
