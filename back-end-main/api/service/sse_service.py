import hashlib

import pandas as pd
from sqlalchemy import MetaData, Table, create_engine, func, inspect, select, update
from sqlalchemy.orm import Session, sessionmaker

from service.database_service import (
    create_table_session,
    get_cloud_database_path,
    get_database_path,
    get_database_user_id,
    get_index_column_table_object,
    get_primary_key,
    get_sensitive_columns,
)


def update_hash_column(
    session_cloud_db,
    table_cloud_db,
    primary_key_data,
    raw_data,
):

    for (primary_key_value, row) in zip(primary_key_data, range(raw_data.shape[0])):
        record = raw_data.iloc[row].values
        record = list(record)
        new_record = str(record)
        hashed_line = hashlib.sha256(new_record.encode("utf-8")).hexdigest()

        stmt = (
            update(table_cloud_db)
            .where(table_cloud_db.c[0] == primary_key_value)
            .values(line_hash=hashed_line)
        )

        session_cloud_db.execute(stmt)

    session_cloud_db.commit()
    session_cloud_db.close()


def generate_hash_rows(
    id_db_user: int, id_db: int, table_name: str, result_query: list[dict]
) -> tuple[int, str]:

    # Get client database path
    src_client_db_path = get_database_path(id_db)
    if not src_client_db_path:
        return (404, "database_not_found")

    # Check user authorization
    if get_database_user_id(id_db=id_db) != id_db_user:
        return (401, "user_unauthorized")

    # Get primary key name of client database
    primary_key_name = get_primary_key(id_db=id_db, table_name=table_name)

    # Get sensitve columns of table
    sensitive_columns = get_sensitive_columns(
        id_db_user=id_db_user, id_db=id_db, table_name=table_name
    )[0]
    client_columns_list = [primary_key_name] + sensitive_columns

    # Get Cloud Database Path
    src_cloud_db_path = get_cloud_database_path(id_db=id_db)

    # Create table object of Cloud Database and
    # session of Cloud Database to run sql operations
    table_cloud_db, session_cloud_db = create_table_session(
        src_db_path=src_cloud_db_path, table_name=table_name
    )

    # Transform query rows to dataframe
    raw_data = pd.DataFrame(result_query, columns=client_columns_list)
    primary_key_data = raw_data[primary_key_name]
    raw_data.pop(primary_key_name)

    update_hash_column(
        session_cloud_db=session_cloud_db,
        table_cloud_db=table_cloud_db,
        primary_key_data=primary_key_data,
        raw_data=raw_data,
    )


def generate_hash_column(
    id_db_user: int, id_db: int, table_name: str
) -> tuple[int, str]:

    # Get client database path
    src_client_db_path = get_database_path(id_db)
    if not src_client_db_path:
        return (404, "database_not_found")

    # Check user authorization
    if get_database_user_id(id_db=id_db) != id_db_user:
        return (401, "user_unauthorized")

    # Get primary key name of client database
    primary_key_name = get_primary_key(id_db=id_db, table_name=table_name)

    # Get sensitve columns of table
    sensitive_columns = get_sensitive_columns(
        id_db_user=id_db_user, id_db=id_db, table_name=table_name
    )[0]
    client_columns_list = [primary_key_name] + sensitive_columns
    print(client_columns_list)

    # Create table object of Client Database and
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_db_path=src_client_db_path,
        table_name=table_name,
        columns_list=client_columns_list,
    )

    # Get Cloud Database Path
    src_cloud_db_path = get_cloud_database_path(id_db=id_db)

    # Create table object of Cloud Database and
    # session of Cloud Database to run sql operations
    table_cloud_db, session_cloud_db = create_table_session(
        src_db_path=src_cloud_db_path, table_name=table_name
    )

    # Commit changes on Cloud Database
    session_cloud_db.commit()
    session_cloud_db.close()

    # Generate hashs
    size = 1000
    statement = select(table_client_db)
    results_proxy = session_client_db.execute(statement)  # Proxy to get data on batch
    results = results_proxy.fetchmany(size)  # Getting data

    while results:
        from_db = []

        for result in results:
            from_db.append(list(result))

        session_client_db.close()

        raw_data = pd.DataFrame(from_db, columns=client_columns_list)
        primary_key_data = raw_data[primary_key_name]
        raw_data.pop(primary_key_name)

        results = results_proxy.fetchmany(size)  # Getting data

        update_hash_column(
            session_cloud_db=session_cloud_db,
            table_cloud_db=table_cloud_db,
            primary_key_data=primary_key_data,
            raw_data=raw_data,
        )


def include_hash_rows(id_db_user, id_db, table_name, hash_rows):

    # Get Cloud Database Path
    src_cloud_db_path = get_cloud_database_path(id_db=id_db)
    if not src_cloud_db_path:
        (404, "database_not_found")

    # Check authorization user
    if get_database_user_id(id_db=id_db) != id_db_user:
        (401, "user_unauthorized")

    # Create table object of Cloud Database and
    # session of Cloud Database to run sql operations
    table_cloud_db, session_cloud_db = create_table_session(
        src_db_path=src_cloud_db_path,
        table_name=table_name,
    )

    # Get primary key column name
    primary_key_name = get_primary_key(id_db=id_db, table_name=table_name)

    # Get primary key index in table object
    index_primary_key = get_index_column_table_object(
        table_object=table_cloud_db, column_name=primary_key_name
    )

    for row in hash_rows:
        statement = (
            update(table_cloud_db)
            .where(table_cloud_db.c[index_primary_key] == row[primary_key_name])
            .values(line_hash=row["line_hash"])
        )

        session_cloud_db.execute(statement)

    session_cloud_db.commit()

    return (200, "hash_included")


def show_rows_hash(
    id_db_user: int, id_db: int, table_name, page: int, per_page: int
) -> tuple:
    """
    This function gets rows hash on Cloud Database.

    Parameters
    ----------
    id_db_user : int
        Database User ID.

    id_db : int
        Client Database ID.

    table_name : str
        Table name.

    page : int
        Page to paginate.

    per_page : int
        Rows number per query.

    Returns
    -------
    tuple
        (query_results, primary_key_value_min_limit, primary_key_value_max_limit)
    """

    # Get Cloud Database Path
    src_cloud_db_path = get_cloud_database_path(id_db=id_db)
    if not src_cloud_db_path:
        (None, None, None)

    # Check authorization user
    if get_database_user_id(id_db=id_db) != id_db_user:
        (None, None, None)

    # Get columns to encrypt
    primary_key_name = get_primary_key(id_db=id_db, table_name=table_name)

    # Create table object of Cloud Database and
    # session of Cloud Database to run sql operations
    table_cloud_db, session_cloud_db = create_table_session(
        src_db_path=src_cloud_db_path,
        table_name=table_name,
        columns_list=[primary_key_name, "line_hash"],
    )

    # Get limits primary key value
    primary_key_value_min_limit = session_cloud_db.query(
        func.min(table_cloud_db.c[0])
    ).scalar()
    primary_key_value_max_limit = session_cloud_db.query(
        func.max(table_cloud_db.c[0])
    ).scalar()

    # Run paginate
    query = session_cloud_db.query(table_cloud_db).filter(
        table_cloud_db.c[0] >= primary_key_value_min_limit + (page * per_page),
        table_cloud_db.c[0] <= primary_key_value_min_limit + ((page + 1) * per_page),
    )

    query_results = {}
    query_results["primary_key"] = []
    query_results["row_hash"] = []
    for row in query:
        query_results["primary_key"].append(row[0])
        query_results["row_hash"].append(row[1])

    session_cloud_db.commit()
    session_cloud_db.close()

    return (query_results, primary_key_value_min_limit, primary_key_value_max_limit)
