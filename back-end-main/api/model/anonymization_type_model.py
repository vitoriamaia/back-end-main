from flask_login import UserMixin

from controller import db, ma


class AnonymizationType(db.Model, UserMixin):
    __tablename__ = "anonymization_types"

    id = db.Column(db.Integer, nullable=False, autoincrement=True, primary_key=True)
    name = db.Column(db.String(100), nullable=False)

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f"<AnonymizationType : {self.name}>"


class AnonymizationTypeSchema(ma.Schema):
    class Meta:
        fields = ("id", "name")


anonymization_type_share_schema = AnonymizationTypeSchema()
anonymization_types_share_schema = AnonymizationTypeSchema(many=True)
