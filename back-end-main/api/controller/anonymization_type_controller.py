from flask import jsonify, request

from controller import app
from service.anonymization_type_service import (
    add_anonymization_type,
    delete_anonymization_type,
    get_anonymizations_type,
)
from service.authenticate import jwt_required


@app.route("/getAnonymizationType", methods=["GET"])
@jwt_required
def getAnonymizationType(current_user):
    try:
        registered_anonymization_types, status_code = get_anonymizations_type()

        if not registered_anonymization_types:
            return jsonify({"message": "anonymization_types_invalid_data"}), 400

        return jsonify(registered_anonymization_types), status_code
    except:
        return jsonify({"message": "anonymization_types_invalid_data"}), 500


@app.route("/addAnonymizationType", methods=["POST"])
@jwt_required
def addAnonymizationType(current_user):
    try:
        anonymization_type_name = request.json.get("name")

        status_code, response_message = add_anonymization_type(
            current_user=current_user, anonymization_type_name=anonymization_type_name
        )

        return jsonify({"message": response_message}), status_code
    except:
        return jsonify({"message": "anonymization_type_not_add"}), 500


@app.route("/deleteAnonymizationType", methods=["DELETE"])
@jwt_required
def deleteAnonymizationType(current_user):
    try:
        id_anonymization_type = request.json.get("id")

        anonymization_type_name = request.json.get("name")

        status_code, response_message = delete_anonymization_type(
            current_user=current_user,
            id_anonymization_type=id_anonymization_type,
            anonymization_type_name=anonymization_type_name,
        )

        return jsonify({"message": response_message}), status_code
    except:
        return jsonify({"message": "anonymization_type_not_deleted"}), 500
