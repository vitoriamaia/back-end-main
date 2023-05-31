from flask_login import UserMixin

from controller import db, ma


class Database(db.Model, UserMixin):
    __tablename__ = "databases"

    id = db.Column(db.Integer, nullable=False, autoincrement=True, primary_key=True)
    id_user = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    id_db_type = db.Column(
        db.Integer, db.ForeignKey("valid_databases.id"), nullable=False
    )
    name = db.Column(db.String(200), nullable=False)
    host = db.Column(db.String(200), nullable=False)
    user = db.Column(db.String(200), nullable=False)
    port = db.Column(db.Integer, nullable=False)
    password = db.Column(db.String(200), nullable=False)
    ssh = db.Column(db.String(500))

    def __init__(self, id_user, id_db_type, name, host, user, port, password, ssh):
        self.id_user = id_user
        self.id_db_type = id_db_type
        self.name = name
        self.host = host
        self.user = user
        self.port = port
        self.password = password
        self.ssh = ssh

    def __repr__(self):
        return f"<Database : {self.name}>"


class DatabaseSchema(ma.Schema):
    class Meta:
        fields = (
            "id",
            "id_user",
            "id_db_type",
            "name",
            "host",
            "user",
            "port",
            "password",
            "ssh",
        )


database_share_schema = DatabaseSchema()
databases_share_schema = DatabaseSchema(many=True)
