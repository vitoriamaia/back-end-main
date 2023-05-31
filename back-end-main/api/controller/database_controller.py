from flask import jsonify, request

from controller import app
from service.authenticate import jwt_required
from service.database_service import (
    add_database,
    delete_database,
    get_database_columns_types,
    get_database_tables,
    get_databases,
    get_sensitive_columns,
    show_database,
    test_connection_database,
)


@app.route("/getDatabases", methods=["GET"])
@jwt_required
def getDatabases(current_user):
    try:
        result_databases, status_code = get_databases(id_db_user=current_user.id)

        return jsonify(result_databases), status_code
    except:
        return jsonify({"message": "databases_invalid_data"}), 400


@app.route("/addDatabase", methods=["POST"])
@jwt_required
def addDatabase(current_user):
    try:
        id_db_type = request.json.get("id_db_type")
        name_db = request.json.get("name")
        host_db = request.json.get("host")
        username_db = request.json.get("user")
        port_db = request.json.get("port")
        password_db = request.json.get("password")

        status_code, response_message = add_database(
            id_user=current_user.id,
            id_db_type=id_db_type,
            name_db=name_db,
            host_db=host_db,
            username_db=username_db,
            port_db=port_db,
            password_db=password_db,
        )

        return jsonify({"message": response_message}), status_code
    except:
        return jsonify({"message": "database_invalid_data"}), 400


@app.route("/deleteDatabase", methods=["DELETE"])
@jwt_required
def deleteDatabase(current_user):
    try:
        id_db = request.json.get("id_db")

        status_code, response_message = delete_database(
            id_db_user=current_user.id, id_db=id_db
        )

        return jsonify({"message": response_message}), status_code
    except:
        return jsonify({"message": "database_not_deleted"}), 500


@app.route("/testConnectionDatabase", methods=["POST"])
@jwt_required
def test_connection(current_user):
    try:
        id_db = request.json.get("id_db")

        src_db_path = None
        if not id_db:
            src_db_path = "{}://{}:{}@{}:{}/{}".format(
                request.json.get("type").lower(),
                request.json.get("user"),
                request.json.get("password"),
                request.json.get("host"),
                request.json.get("port"),
                request.json.get("name"),
            )

        status_code, response_message = test_connection_database(
            id_db=id_db, src_db_path=src_db_path
        )

        return jsonify({"message": response_message}), status_code
    except:
        return jsonify({"message": "database_invalid_data"}), 500


@app.route("/tablesDatabase", methods=["POST"])
@jwt_required
def tablesDatabase(current_user):
    try:
        id_db = request.json.get("id_db")

        tables_names, status_code, response_message = get_database_tables(
            id_db_user=current_user.id, id_db=id_db
        )

        if not tables_names:
            return jsonify({"message": response_message}), status_code

        return jsonify(tables_names), status_code
    except:
        return jsonify({"message": "database_invalid_data"}), 400


@app.route("/columnsDatabase", methods=["POST"])
@jwt_required
def columnsDatabase(current_user):
    try:
        # Get Database ID
        id_db = request.json.get("id_db")

        # Get table name
        table_name = request.json.get("table")

        columns, status_code, response_message = get_database_columns_types(
            id_db_user=current_user.id, id_db=id_db, table_name=table_name
        )

        if not columns:
            return jsonify({"message": response_message}), status_code

        return jsonify(columns), status_code
    except:
        return jsonify({"message": "database_invalid_data"}), 400


@app.route("/getSensitiveColumns", methods=["POST"])
@jwt_required
def getSensitiveColumns(current_user):
    try:
        id_db = request.json.get("id_db")
        table_name = request.json.get("table")

        sensitive_columns, status_code, response_message = get_sensitive_columns(
            id_db_user=current_user.id, id_db=id_db, table_name=table_name
        )

        if not sensitive_columns:
            return jsonify({"message": response_message}), status_code

        return jsonify(sensitive_columns), 200
    except:
        return jsonify({"message": "database_invalid_data"}), 400


@app.route("/showDatabase", methods=["POST"])
@jwt_required
def showDatabase(current_user):
    try:
        id_db = request.json.get("id_db")

        src_db_path = None
        if not id_db:
            src_db_path = "{}://{}:{}@{}:{}/{}".format(
                request.json.get("type").lower(),
                request.json.get("user"),
                request.json.get("password"),
                request.json.get("host"),
                request.json.get("port"),
                request.json.get("name"),
            )

        table_name = request.json.get("table")
        page = request.json.get("page")
        per_page = request.json.get("per_page")

        # Get data and show
        query_rows, status_code, response_message = show_database(
            id_db=id_db,
            src_db_path=src_db_path,
            table_name=table_name,
            page=page,
            per_page=per_page,
        )

        if not query_rows and type(query_rows).__name__ != "list":
            return jsonify({"message": response_message}), status_code

        return jsonify(query_rows), status_code
    except:
        return jsonify({"message": "database_invalid_data"}), 400
