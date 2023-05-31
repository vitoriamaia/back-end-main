from sqlalchemy import select

from config import HOST, PASSWORD_DATABASE, PORT, TYPE_DATABASE, USER_DATABASE
from model.database_key_model import DatabaseKey, database_key_share_schema
from model.database_model import Database, database_share_schema
from service.anonymization_service import anonymization_database_rows
from service.database_service import (
    create_table_session,
    get_cloud_database_path,
    get_database_columns_types,
    get_database_path,
    get_database_user_id,
    get_index_column_table_object,
    get_primary_key,
    get_sensitive_columns,
)
from service.rsa_service import decrypt_dict, encrypt_database_rows, loadKeys
from service.sse_service import generate_hash_rows


def row_search(
    id_db_user: int, id_db: int, table_name: str, search_type: str, search_value: any
) -> dict:
    """
    This function returns a row given with your decrypted sensitive
    data along with the non-sensitive data. The search for the row
    is done through a given valueof the primary key or the hash of
    the row.

    Parameters
    ----------
    id_db_user : int
        Database User ID.

    id_db : int
        ID of the database where the row will be searched.

    table_name : str
        Name of the table where the row to be searched is located.

    search_type : str
        Indicates the type of value used to search for the row
        ("primary key" or "hash_line").

    search_value : int or str
        If the row search uses the primary key value then the
        search_value will be an integer representing the primary
        key value. If the search is by hash, then the search_value
        will be a string with the hash value.

    Returns
    -------
    tuple
        (status code, response message).
    """

    # Get client database path
    src_client_db_path = get_database_path(id_db=id_db)
    if not src_client_db_path:
        return (None, 404, "database_not_found")

    # Check user authorization
    if get_database_user_id(id_db=id_db) != id_db_user:
        return (401, "user_unauthorized")

    # Get path of Cloud Database
    src_cloud_db_path = get_cloud_database_path(id_db=id_db)

    # Create table object of Cloud Database and
    # session of Cloud Database to run sql operations
    table_cloud_db, session_cloud_db = create_table_session(
        src_db_path=src_cloud_db_path, table_name=table_name
    )

    # Get primary key name of Client Database
    primary_key_name = get_primary_key(id_db=id_db, table_name=table_name)

    # Searching to row on Cloud Database
    if search_type == "primary_key":
        index_column_to_search = get_index_column_table_object(
            table_object=table_cloud_db,
            column_name=primary_key_name,
        )
        query_sensitive_data = (
            session_cloud_db.query(table_cloud_db)
            .filter(table_cloud_db.c[index_column_to_search] == search_value)
            .all()
        )
    elif search_type == "hash":
        index_column_to_search = get_index_column_table_object(
            table_object=table_cloud_db,
            column_name="line_hash",
        )
        print(index_column_to_search)
        query_sensitive_data = (
            session_cloud_db.query(table_cloud_db)
            .filter(table_cloud_db.c[index_column_to_search] == search_value)
            .all()
        )
    else:
        return (None, 400, "search_invalid_data")

    if not query_sensitive_data:
        return (None, 404, "row_not_found")

    # Transform sensitive data query to dictionary
    dict_sensitive_data = [row._asdict() for row in query_sensitive_data][0]

    print(f"....-> {query_sensitive_data}")
    print(f"*****-> {dict_sensitive_data}")

    # Remove primary key and hash value
    primary_key_value = dict_sensitive_data[f"{primary_key_name}"]
    hash_value = dict_sensitive_data["line_hash"]
    remove_primary_key_response = dict_sensitive_data.pop(primary_key_name, None)
    remove_line_hash_response = dict_sensitive_data.pop("line_hash", None)

    if remove_primary_key_response == None or remove_line_hash_response == None:
        return (None, 500, "internal_server_erro5585r")

    # Load rsa keys
    result_keys = database_key_share_schema.dump(
        DatabaseKey.query.filter_by(id_db=id_db).first()
    )

    if not result_keys:
        return (None, 500, "database_keys_not_foundr")

    _, private_key = loadKeys(
        publicKeyStr=result_keys["public_key"], privateKeyStr=result_keys["private_key"]
    )

    # Decrypt sensitive
    dict_decrypted_sensitive_data = decrypt_dict(dict_sensitive_data, private_key)

    # Create table object of Cloud Database and
    # session of Cloud Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_db_path=src_client_db_path, table_name=table_name
    )

    # Searching to row on Client Database
    index_column_to_search = get_index_column_table_object(
        table_object=table_client_db,
        column_name=primary_key_name,
    )
    query_non_sensitive_data = (
        session_client_db.query(table_client_db)
        .filter(table_client_db.c[index_column_to_search] == search_value)
        .all()
    )

    if not query_non_sensitive_data:
        return (None, 404, "row_not_found")

    # Transform non-sensitive data query to dictionary
    dict_result_data = [row._asdict() for row in query_non_sensitive_data][0]
    print(f"====-> {dict_decrypted_sensitive_data}")
    print(f"$$$$-> {dict_result_data}")

    # Insert decrypted sensitive data in non-sensitive data dictionary
    for key in dict_decrypted_sensitive_data.keys():
        dict_result_data[key] = dict_decrypted_sensitive_data[key]

    # Get date type columns on news rows of Client Database
    data_type_keys = []
    for key in dict_result_data.keys():
        type_data = str(type(dict_result_data[key]).__name__)
        if type_data == "date":
            data_type_keys.append(key)

    # Fix columns date types of row
    for key in data_type_keys:
        dict_result_data[key] = dict_result_data[key].strftime("%Y-%m-%d")

    # Fix columns types of row
    columns_types = get_database_columns_types(
        id_db_user=id_db_user, id_db=id_db, table_name=table_name
    )[0]

    for key in columns_types.keys():
        column_type = columns_types[key].split("(")[0]

        if column_type == "INTEGER":
            dict_result_data[key] = int(dict_result_data[key])
        elif column_type == "VARCHAR":
            dict_result_data[key] = str(dict_result_data[key])
        else:
            pass

    return (dict_result_data, 200, "row_found")


def process_updates(
    id_db_user: int, id_db: int, table_name: str, primary_key_list: list
) -> tuple:

    # Check request body
    if (
        (id_db == None)
        or (table_name == None)
        or (primary_key_list == None)
        or (len(primary_key_list) == 0)
    ):
        return (400, "invalid_updates_data5")

    # Get client database path
    src_client_db_path = get_database_path(id_db)
    if not src_client_db_path:
        return (404, "database_not_found")

    # Check user authorization
    if get_database_user_id(id_db=id_db) != id_db_user:
        return (401, "user_unauthorized")

    # Create table object of Client Database and
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_db_path=src_client_db_path, table_name=table_name
    )

    # Get database information by id
    src_cloud_db_path = get_cloud_database_path(id_db=id_db)

    # Get sensitive columns of Client Database
    sensitive_columns = get_sensitive_columns(
        id_db_user=id_db_user, id_db=id_db, table_name=table_name
    )[0]

    # Get original rows ​​that have been updated
    rows_list = []

    for primary_value in primary_key_list:
        found_row = row_search(
            id_db_user=id_db_user,
            id_db=id_db,
            table_name=table_name,
            search_type="primary_key",
            search_value=primary_value,
        )
        found_row = found_row[0]
        print(f"\n---->>> {found_row}\n")

        anonymized_row = found_row.copy()
        anonymized_row, status_code, _ = anonymization_database_rows(
            id_db=id_db,
            table_name=table_name,
            rows_to_anonymization=[anonymized_row],
            insert_database=False,
        )
        anonymized_row = anonymized_row[0]
        print(f"\n---->>>> {anonymized_row}\n")

        stmt = select(table_client_db).where(table_client_db.c[0] == primary_value)
        client_row = session_client_db.execute(stmt)
        client_row = [row._asdict() for row in client_row][0]

        for sensitive_column in sensitive_columns:

            if type(client_row[sensitive_column]).__name__ == "date":
                if anonymized_row[sensitive_column] != client_row[
                    sensitive_column
                ].strftime("%Y-%m-%d"):
                    found_row[sensitive_column] = client_row[sensitive_column]
                    print(f"###TROCOU### -> {sensitive_column}")
            else:
                if anonymized_row[sensitive_column] != client_row[sensitive_column]:
                    found_row[sensitive_column] = client_row[sensitive_column]
                    print(f"###TROCOU### -> {sensitive_column}")

        rows_list.append(found_row)

    encrypt_database_rows(
        id_db_user=id_db_user,
        id_db=id_db,
        rows_to_encrypt=rows_list.copy(),
        table_name=table_name,
        update_database=True,
    )

    anonymized_rows, _, _ = anonymization_database_rows(
        id_db=id_db,
        table_name=table_name,
        rows_to_anonymization=rows_list.copy(),
        insert_database=True,
    )

    generate_hash_rows(
        id_db_user=id_db_user,
        id_db=id_db,
        table_name=table_name,
        result_query=anonymized_rows,
    )

    if status_code == 200:
        return (200, "updates_processed")


def process_deletions(
    id_db_user: int, id_db: int, table_name: str, primary_key_list: list
) -> tuple:
    """
    This function processes deletions of client database.

    Parameters
    ----------
    id_db_user : int
        Database User ID.

    id_db : int
        ID of the database where it will be processed deletions.

    table_name : str
        Name of the table where it will be processed deletions.

    primary_key_list : list
        Primary key values of rows ​​that have been removed

    Returns
    -------
    tuple
        (status code, response message).
    """

    # Check request body
    if (
        (id_db == None)
        or (table_name == None)
        or (primary_key_list == None)
        or (len(primary_key_list) == 0)
    ):
        return (400, "invalid_removal_data ")

    # Check user authorization
    if get_database_user_id(id_db=id_db) != id_db_user:
        return (401, "user_unauthorized")

    # Get database information by id
    src_cloud_db_path = get_cloud_database_path(id_db=id_db)

    # Create table object of Cloud Database and
    # session of Cloud Database to run sql operations
    table_cloud_db, session_cloud_db = create_table_session(
        src_db_path=src_cloud_db_path, table_name=table_name
    )

    for primary_key in primary_key_list:
        session_cloud_db.query(table_cloud_db).filter(
            table_cloud_db.c[0] == primary_key
        ).delete()

    session_cloud_db.commit()
    session_cloud_db.close()

    return (200, "deletions_processed")
