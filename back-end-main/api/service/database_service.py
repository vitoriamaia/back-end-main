from sqlalchemy import MetaData, Table, create_engine, inspect
from sqlalchemy.orm import Session
from sqlalchemy_utils import database_exists

import service
from config import (
    HOST,
    NAME_DATABASE,
    PASSWORD_DATABASE,
    PORT,
    TYPE_DATABASE,
    USER_DATABASE,
)
from controller import db
from model.anonymization_model import Anonymization, anonymizations_share_schema
from model.database_key_model import DatabaseKey
from model.database_model import Database, database_share_schema, databases_share_schema
from model.valid_database_model import ValidDatabase, valid_databases_share_schema


def get_database_path(id_db: int) -> str:
    """
    This function get database path by database ID.

    Parameters:
    ----------
    id_db : int
        Database ID.

    Returns:
    -------
    str
        Database path.
    """

    # Get database informations
    result_database = database_share_schema.dump(
        Database.query.filter_by(id=id_db).first()
    )
    if not result_database:
        return None

    # Get valid database name
    db_type_name = (
        ValidDatabase.query.filter_by(id=result_database["id_db_type"]).first().name
    )
    if not db_type_name:
        return None

    # Define database path
    src_db_path = "{}://{}:{}@{}:{}/{}".format(
        db_type_name,
        result_database["user"],
        result_database["password"],
        result_database["host"],
        result_database["port"],
        result_database["name"],
    )

    return src_db_path


def get_database_user_id(id_db: int) -> int:
    """
    This function get database user ID

    Parameters:
    ----------
    id_db : int
        Database ID.

    Returns:
    -------
    int
        Database user ID
    """

    # Get database informations
    result_database = database_share_schema.dump(
        Database.query.filter_by(id=id_db).first()
    )

    if not result_database:
        return None

    return result_database["id_user"]


def get_cloud_database_path(id_db: int) -> str:
    """
    This function get cloud database path by client database ID.

    Parameters:
    ----------
    id_db : int
        Client database ID.

    Returns:
    -------
    str
        Path cloud database.
    """

    # Get database informations
    result_client_database = database_share_schema.dump(
        Database.query.filter_by(id=id_db).first()
    )

    if not result_client_database:
        return None

    # Define cloud database path
    src_cloud_db_path = "{}://{}:{}@{}:{}/{}".format(
        TYPE_DATABASE,
        USER_DATABASE,
        PASSWORD_DATABASE,
        HOST,
        PORT,
        f"{result_client_database['name']}_cloud_U{result_client_database['id_user']}DB{result_client_database['id']}",
    )

    return src_cloud_db_path


def get_database_columns(
    id_db: int = None, src_db_path: str = None, table_name: str = None
) -> list[str]:
    """
    This function get database columns names.

    Parameters:
    ----------
    id_db : int
        Database ID if registered.

    src_db_path : str
        Database path.

    table_name : str
        Table name.

    Returns:
    -------
    list[str]
        Database columns names.
    """

    # Get database path by database ID
    if id_db:
        src_db_path = get_database_path(id_db=id_db)

    # Get database columns
    columns_names = []

    engine_db = create_engine(src_db_path)

    columns_table = inspect(engine_db).get_columns(table_name)

    for c in columns_table:
        columns_names.append(str(c["name"]))

    return columns_names


def get_primary_key(
    id_db: int = None, src_db_path: str = None, table_name: str = None
) -> None | str:
    """
    This function get column name of primary key.

    Parameters
    ----------
     id_db : int
        Database ID if registered.

    src_db_path : str
        Database path.

    table_name : str
        Table name.

    Returns
    -------
    None | str
        If an error occurs, the return will be: None.
        Else the return will be: primary key name.
    """

    # Get database path by id
    if id_db:
        src_db_path = get_database_path(id_db=id_db)
        if not src_db_path:
            return None

    # Create table object of database
    table_object_db, _ = create_table_session(
        id_db=id_db, src_db_path=src_db_path, table_name=table_name
    )

    return [key.name for key in inspect(table_object_db).primary_key][0]


def create_table_session(
    id_db: int = None,
    src_db_path: str = None,
    table_name: str = None,
    columns_list: list = None,
) -> None | tuple[Table, Session]:
    """
    This function create table object and table session to execute SQL operations.

    Parameters
    ----------
    id_db : int
        Database ID if registered.

    src_db_path : str
        Database path.

    table_name : str
        Table name.

    columns_list : list
        Columns to table object.

    Returns
    -------
    None | str
        If an error occurs, the return will be: None.
        Else the return will be: (table object, table session)
    """

    # Get database path by id
    if id_db:
        src_db_path = get_database_path(id_db)
        if not src_db_path:
            return (None, None)

    # Create engine, reflect existing columns
    engine_db = create_engine(src_db_path)
    engine_db._metadata = MetaData(bind=engine_db)

    # Get columns from existing table
    engine_db._metadata.reflect(engine_db)

    if columns_list == None:
        columns_list = get_database_columns(
            id_db=id_db, src_db_path=src_db_path, table_name=table_name
        )

    engine_db._metadata.tables[table_name].columns = [
        i
        for i in engine_db._metadata.tables[table_name].columns
        if (i.name in columns_list)
    ]

    # Create table object of Client Database
    table_object_db = Table(table_name, engine_db._metadata)

    # Create session of Client Database to run sql operations
    session_db = Session(engine_db)

    return (table_object_db, session_db)


def get_index_column_table_object(
    table_object: Table,
    column_name: list = None,
) -> None | int:
    """
    This function get column index in table object.

    Parameters
    ----------
    table_object: Table
        Table object.

    column_name : list
        Column name.

    Returns
    -------
    None | str
        If an error occurs, the return will be: None.
        Else the return will be: Index Column
    """

    # Search index of column
    index = 0
    for column in table_object.c:
        if column.name == column_name:
            return index
        index += 1

    return None


def get_databases(id_db_user: int) -> tuple[list[dict], int]:
    """
    This function returns all databases of a registered user

    Parameters
    ----------
    id_db_user : int
        Database User ID

    Returns
    -------
    tuple[list[dict], int]
        (Databases dictionary, status code)
    """

    result_databases = databases_share_schema.dump(
        Database.query.filter_by(id_user=id_db_user).all()
    )

    result_valid_databases = valid_databases_share_schema.dump(
        ValidDatabase.query.all()
    )

    for database in result_databases:
        for type in result_valid_databases:
            if type["id"] == database["id_db_type"]:
                database["name_db_type"] = type["name"]

    return result_databases, 200


def add_database(
    id_user: int,
    id_db_type: int,
    name_db: str,
    host_db: str,
    username_db: str,
    port_db: int | str,
    password_db: str,
) -> tuple[int, str]:
    """
    This function add clients databases.

    Parameters
    ----------
    id_db_user : int
        Database User ID

    id_db_type : int
        Databases type ID

    name_db: str
        Database name

    username_db : str
        Name of Database User

    port_db : str
        Acess port of database

    password_db : str
        Database password

    Returns
    -------
    tuple[int, str]
        (status code, response message)
    """

    try:
        database = Database(
            id_db_type=id_db_type,
            id_user=id_user,
            name=name_db,
            host=host_db,
            user=username_db,
            port=port_db,
            password=password_db,
            ssh="",
        )
        db.session.add(database)
        db.session.flush()
    except:
        db.session.rollback()
        return (400, "database_invalid_data")

    try:
        # Generate rsa keys and save them
        publicKeyStr, privateKeyStr = service.rsa_service.generateKeys()
        database_keys = DatabaseKey(database.id, publicKeyStr, privateKeyStr)
        db.session.add(database_keys)
    except:
        db.session.rollback()
        return (500, "database_keys_not_added")

    # Commit updates
    db.session.commit()

    return (201, "database_added")


def delete_database(id_db_user: int, id_db: int) -> tuple[int, str]:
    """
    This function delete clients databases.

    Parameters
    ----------
    id_db_user : int
        Database User ID

    id_db : int
        Databases ID

    Returns
    -------
    tuple[int, str]
        (status code, response message)
    """

    database = Database.query.filter_by(id=id_db).first()

    if not database:
        return (404, "database_not_found")

    if database.id_user != id_db_user:
        return (401, "user_unauthorized")

    try:
        DatabaseKey.query.filter_by(id_db=id_db).delete()
        db.session.delete(database)
    except:
        db.session.rollback()
        return (500, "database_not_deleted")

    db.session.commit()

    result = database_share_schema.dump(Database.query.filter_by(id=id).first())

    if not result:
        return (200, "database_deleted")
    else:
        return (500, "database_not_deleted")


def test_connection_database(id_db: int, src_db_path: str) -> tuple[int, str]:
    """
    This function test connection with database.

    Parameters:
    ----------
    id_db : int
        Database ID if registered.

    src_db_path : str
        Database path.

    Returns:
    -------
    tuple[int, str]
        (status code, response message).
    """

    # Get database path by id
    if id_db:
        src_db_path = get_database_path(id_db=id_db)
        if not src_db_path:
            return (404, "database_not_found")
    try:
        # Create database engine to connection
        engine = create_engine(src_db_path)
    except:
        return (409, "database_not_connected")

    # Test connection with database
    if database_exists(engine.url):
        return (200, "database_connected")
    else:
        return (409, "database_not_connected")


def get_database_tables(
    id_db_user: int,
    id_db: int,
) -> tuple[None, int, str] | tuple[list[str], int, str]:
    """
    This function get tables names

    Parameters:
    ----------
    id_db_user : int
        Database user ID

    id_db : int
        Database ID.

    Returns
    -------
    tuple[None, int, str] | tuple[list[str], int, str]
        If an error occurs, the return will be: (None, status code, response message)
        Else the return will be: (Tables names, status code, response message)
    """

    # Get database path by id
    if id_db:
        src_db_path = get_database_path(id_db=id_db)
        if not src_db_path:
            return (None, 404, "database_not_found")

    # Check user authorization
    if get_database_user_id(id_db=id_db) != id_db_user:
        return (None, 401, "user_unauthorized")

    # Create engine to connect
    try:
        engine_db = create_engine(src_db_path)
    except:
        return (None, 400, "database_invalid_data")

    # Get tables names
    tables_names = list(engine_db.table_names())

    return (tables_names, 200, None)


def get_database_columns_types(
    id_db_user: int, id_db: int, table_name: str
) -> tuple[None, int, str] | tuple[dict[str, str], int, str]:
    """
    This function returns table columns with their types.

    Parameters
    ----------
    id_db_user : int
        Database user ID

    id_db : int
        Database ID.

    table_name : str
        Table name.

    Returns
    -------
    tuple[None, int, str] | tuple[dict[str, str], int, str]:
        If an error occurs, the return will be: (None, status code, response message).
        Else the return will be: (columns names, status code, response message)
    """

    # Get database path by id
    if id_db:
        src_db_path = get_database_path(id_db=id_db)
        if not src_db_path:
            return (None, 404, "database_not_found")

    # Check user authorization
    if get_database_user_id(id_db=id_db) != id_db_user:
        return (None, 401, "user_unauthorized")

    # Create connection to database
    engine_db = create_engine(src_db_path)

    # Get columns and their types
    columns = {}

    insp = inspect(engine_db)
    columns_table = insp.get_columns(table_name)

    for c in columns_table:
        columns[f"{c['name']}"] = str(c["type"])

    return (columns, 200, None)


def get_sensitive_columns(
    id_db_user: int, id_db: int, table_name: str
) -> tuple[None, int, str] | tuple[list, int, str]:
    """
    This function returns sensitive columns of a table.

    Parameters
    ----------
    id_db_user : int
        Database user ID.

    id_db : int
        Database ID.

    table_name : str
        Table name.

    Returns
    -------
    tuple[None, int, str] | tuple[list, int, str]
        If an error occurs, the return will be: (None, status code, response message).
        Else the return will be: (sensitive columns, status code, None).
    """

    # Get client database path by database ID
    src_client_db_path = get_database_path(id_db)
    if not src_client_db_path:
        return (None, 404, "database_not_found")

    # Check user authorization
    if get_database_user_id(id_db=id_db) != id_db_user:
        return (None, 401, "user_unauthorized")

    # Get chosen columns to anonymize
    lists_columns_anonymizations = anonymizations_share_schema.dump(
        Anonymization.query.filter_by(id_database=id_db, table=table_name).all()
    )
    if not lists_columns_anonymizations:
        return (None, 400, "anonymization_invalid_data")

    sensitive_columns = []
    ids_type_anonymization = []

    # Get sensitive_columns
    for sensitive_column in lists_columns_anonymizations:
        if sensitive_column["columns"]:
            sensitive_columns += sensitive_column["columns"]
            ids_type_anonymization += [sensitive_column["id_anonymization_type"]] * len(
                sensitive_column["columns"]
            )

    return (sensitive_columns, 200, None)


def show_database(
    id_db: int = None,
    src_db_path: str = None,
    table_name: str = None,
    page: int = None,
    per_page: int = None,
) -> tuple[None, int, str] | tuple[list[dict], int, str]:
    """
    This function create table object and table session to execute SQL operations.

    Parameters
    ----------
    id_db : int
        Database ID if registered.

    src_db_path : str
        Database path.

    table_name : str
        Table name.

    page : int
        Page number.

    per_page : int
        Rows number per query.

    Returns
    -------
    tuple[None, int, str] | tuple[list[dict], int, str]:
        If an error occurs, the return will be: (None, status code, response message).
        Else the return will be: (Database rows, status code, response message).
    """

    # Get database path by id
    if id_db:
        src_db_path = get_database_path(id_db)
        if not src_db_path:
            return (None, 404, "database_not_found")

    table_object_db, session_db = create_table_session(
        id_db=id_db, table_name=table_name
    )

    # Run paginate
    query = session_db.query(table_object_db)

    if per_page is not None:
        query = query.limit(per_page)
    if page is not None:
        query = query.offset(page * per_page)

    query_rows = [row._asdict() for row in query]

    return (query_rows, 200, None)
