from flask import jsonify, request

from controller import app
from service.anonymization_service import (
    add_anonymization,
    anonymization_database,
    anonymization_database_rows,
    delete_anonymization,
    get_anonymizations,
)
from service.authenticate import jwt_required


@app.route("/getAnonymization", methods=["GET"])
@jwt_required
def getAnonymization(current_user):
    try:
        registered_anonymizations, status_code = get_anonymizations()

        return jsonify(registered_anonymizations), status_code
    except:
        return jsonify({"message": "anonymizations_invalid_data"}), 500


@app.route("/addAnonymization", methods=["POST"])
@jwt_required
def addAnonymization(current_user):
    try:
        # Get database id to anonymize
        id_db = request.json.get("id_db")

        # Get anonymization type
        id_anonymization_type = request.json.get("id_anonymization_type")

        # Get name table to anonymize
        table_name = request.json.get("table")

        # Get chosen columns to anonymize
        columns_to_anonymize = request.json.get("columns")

        status_code, response_message = add_anonymization(
            id_db=id_db,
            id_anonymization_type=id_anonymization_type,
            table_name=table_name,
            columns_to_anonymize=columns_to_anonymize,
        )

        return jsonify({"message": response_message}), status_code
    except:
        return jsonify({"message": "anonymization_invalid_data"}), 400


@app.route("/deleteAnonymization", methods=["DELETE"])
@jwt_required
def deleteAnonymization(current_user):
    try:
        id_anonymization = request.json.get("id_anonymization")

        status_code, response_message = delete_anonymization(
            id_anonymization=id_anonymization
        )

        return jsonify({"message": response_message}), status_code
    except:
        return jsonify({"message": "anonymization_invalid_data"}), 400


@app.route("/anonymizationDatabaseRows", methods=["POST"])
@jwt_required
def anonymizationDatabaseRows(current_user):
    # Get id of database to anonymize.
    id_db = request.json.get("id_db")

    # Get table_name to encrypt.
    table_name = request.json.get("table_name")

    # Get row of Client Database to anonymization.
    rows_to_anonymization = request.json.get("rows_to_anonymization")

    # Flag to indicate if anonymized rows will be inserted or returned.
    insert_database = request.json.get("insert_database")

    # Run anonymization.
    (
        rows_to_anonymization,
        status_code,
        response_message,
    ) = anonymization_database_rows(
        id_db=id_db,
        table_name=table_name,
        rows_to_anonymization=rows_to_anonymization,
        insert_database=insert_database,
    )

    if not rows_to_anonymization:
        return jsonify({"message": response_message}), status_code

    return jsonify(rows_to_anonymization), status_code


@app.route("/anonymizationDatabase", methods=["POST"])
@jwt_required
def anonymizationDatabase(current_user):
    try:
        # Get id of database to anonymize
        id_db = request.json.get("id_db")

        # Get table_name to encrypt.
        table_name = request.json.get("table")

        # Run anonymization
        status_code, response_message = anonymization_database(
            id_db_user=current_user.id, id_db=id_db, table_name=table_name
        )

        return jsonify({"message": response_message}), status_code
    except:
        return jsonify({"message": "anonymization_invalid_data"}), 400
