from flask import jsonify, request

from controller import app
from service import rsa_service
from service.authenticate import jwt_required


@app.route("/encryptDatabaseRows", methods=["POST"])
@jwt_required
def encryptDatabaseRows(current_user):
    try:
        # Get id of database to encrypt
        id_db = request.json.get("id_db")

        # Rows to encrypt
        rows_to_encrypt = request.json.get("rows_to_encrypt")

        # Get table nam to encrypt
        table_name = request.json.get("table")

        # Get SQL operation flag
        update_database = request.json.get("update_database")

        # Run cryptography
        status_code, response_message = rsa_service.encrypt_database_rows(
            id_db_user=current_user.id,
            id_db=id_db,
            rows_to_encrypt=rows_to_encrypt,
            table_name=table_name,
            update_database=update_database,
        )

        return jsonify({"message": response_message}), status_code
    except:
        return jsonify({"message": "database_invalid_data"}), 400


@app.route("/encryptDatabase", methods=["POST"])
@jwt_required
def encryptDatabase(current_user):
    try:
        # Get id of database to encrypt
        id_db = request.json.get("id_db")

        # Get table name to encrypted
        table_name = request.json.get("table")

        # Run cryptography
        status_code, response_message = rsa_service.encrypt_database(
            id_db_user=current_user.id,
            id_db=id_db,
            table_name=table_name,
        )

        return jsonify({"message": response_message}), status_code
    except:
        return jsonify({"message": "database_invalid_data"}), 400
