from flask import jsonify, request

from config import HOST, PASSWORD_DATABASE, PORT, TYPE_DATABASE, USER_DATABASE
from controller import app, db
from model.database_model import Database, database_share_schema
from model.valid_database_model import ValidDatabase
from service.authenticate import jwt_required
from service.sse_service import include_hash_rows, show_rows_hash


@app.route("/includeHashRows", methods=["POST"])
@jwt_required
def includeHashRows(current_user):
    try:
        id_db = request.json.get("id_db")
        table_name = request.json.get("table")
        hash_rows = request.json.get("hash_rows")

        status_code, response_message = include_hash_rows(
            id_db_user=current_user.id,
            id_db=id_db,
            table_name=table_name,
            hash_rows=hash_rows,
        )

        return jsonify({"message": response_message}), status_code
    except:
        return jsonify({"message": "hash_not_included"}), 400


@app.route("/showRowsHash", methods=["POST"])
@jwt_required
def showRowsHash(current_user):

    id_db = request.json.get("id_db")

    # Get show rows hash
    (
        result_query,
        primary_key_value_min_limit,
        primary_key_value_max_limit,
    ) = show_rows_hash(
        id_db_user=current_user.id,
        id_db=id_db,
        table_name=request.json.get("table"),
        page=request.json.get("page"),
        per_page=request.json.get("per_page"),
    )

    return (
        jsonify(
            {
                "result_query": result_query,
                "primary_key_value_min_limit": primary_key_value_min_limit,
                "primary_key_value_max_limit": primary_key_value_max_limit,
            }
        ),
        200,
    )
