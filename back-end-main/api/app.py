import os

from flask import jsonify, redirect
from flask_migrate import Migrate
from sqlalchemy import create_engine
from sqlalchemy_utils import create_database, database_exists

from config import (
    HOST,
    NAME_DATABASE,
    PASSWORD_DATABASE,
    PORT,
    TYPE_DATABASE,
    USER_DATABASE,
)
from controller import (
    anonymization_controller,
    anonymization_type_controller,
    app,
    cloud_controller,
    database_controller,
    db,
    rsa_controller,
    sse_controller,
    user_controller,
    valid_database_controller,
)
from model import (
    anonymization_model,
    anonymization_type_model,
    database_key_model,
    database_model,
    user_model,
    valid_database_model,
)
from service.database_service import create_table_session, get_index_column_table_object

Migrate(app, db)


@app.shell_context_processor
def make_shell_context():
    return dict(app=app, db=db, User=user_model.User)


@app.route("/")
def index():
    return redirect("https://github.com/FRIDA-LACNIC-UECE", code=302)


@app.route("/test")
def test():

    table_object, session_db = create_table_session(id_db=1, table_name="nivel1")

    print(table_object)
    print("------")
    print(session_db)
    print("------")
    index = get_index_column_table_object(
        table_object=table_object, column_name="altura"
    )
    print(index)
    print("------")
    print(table_object.c[index])

    return (
        jsonify({"message": "done"}),
        200,
    )


if __name__ == "__main__":

    # Define public database path
    src_public_db_path = "{}://{}:{}@{}:{}/{}".format(
        TYPE_DATABASE, USER_DATABASE, PASSWORD_DATABASE, HOST, PORT, NAME_DATABASE
    )

    # Create public database
    engine_dest_db = create_engine(src_public_db_path)
    if not database_exists(engine_dest_db.url):
        create_database(engine_dest_db.url)

        # Initialize migrate
        os.system("flask db init")
        os.system('flask db migrate -m "Initial migration"')
        os.system("flask db upgrade")

    # Run API
    app.run(host="0.0.0.0", port=5000, debug=True)
