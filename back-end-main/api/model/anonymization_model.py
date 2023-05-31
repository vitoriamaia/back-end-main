from flask_login import UserMixin

from controller import db, ma


class Anonymization(db.Model, UserMixin):
    __tablename__ = "anonymizations"

    id = db.Column(db.Integer, nullable=False, autoincrement=True, primary_key=True)
    id_database = db.Column(db.Integer, db.ForeignKey("databases.id"), nullable=False)
    id_anonymization_type = db.Column(
        db.Integer, db.ForeignKey("anonymization_types.id"), nullable=False
    )
    table = db.Column(db.String(150), nullable=False)
    columns = db.Column(db.JSON, nullable=False)

    def __init__(self, id_database, id_anonymization_type, table, columns):
        self.id_database = id_database
        self.id_anonymization_type = id_anonymization_type
        self.table = table
        self.columns = columns

    def __repr__(self):
        return f"<Anonymization : {self.id_anonymization_type} - {self.table} - {self.columns}>"


class AnonymizationSchema(ma.Schema):
    class Meta:
        fields = ("id", "id_database", "id_anonymization_type", "table", "columns")


anonymization_share_schema = AnonymizationSchema()
anonymizations_share_schema = AnonymizationSchema(many=True)
