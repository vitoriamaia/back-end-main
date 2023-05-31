from faker import Faker
from sqlalchemy import select

from service.database_service import (
    create_table_session,
    get_index_column_table_object,
    get_primary_key,
)


def anonymization_rg(seed: int) -> str:
    """
    This function generates new rg.

    Parameters
    ----------
    seed : int
        Seed value.

    Returns
    -------
    str
        New rg generated.
    """

    # Define generator seed
    Faker.seed(seed)

    # Create rg generator and define data language
    faker = Faker(["pt_BR"])

    # Generate new rg
    new_rg = faker.rg()

    return str(new_rg)


def anonymization_data(
    primary_key_name: str,
    rows_to_anonymize: list[dict],
    columns_to_anonymize: list[str],
) -> list[dict]:
    """
    This function anonymizes each column of each given row with
    the RG Anonymizar.

    Parameters
    ----------
    primary_key_name : str
        Primary key column name.

    row_to_anonymize : list[dict]
        Rows provided for anonymization.

    columns_to_anonymize : list[str]
        Column names chosen for anonymization.

    Returns
    -------
    list[dict]
        Anonymized rows
    """

    for number_row in range(len(rows_to_anonymize)):
        # Get primary key value
        primary_key_value = rows_to_anonymize[number_row][primary_key_name]

        # Generate new email
        new_email = anonymization_rg(seed=primary_key_value)

        # Add new email in chosen columns to anonymize
        for column in columns_to_anonymize:
            rows_to_anonymize[number_row][column] = new_email

    return rows_to_anonymize


def anonymization_database_rows(
    src_client_db_path: str,
    table_name: str,
    columns_to_anonymize: list[str],
    rows_to_anonymize: list[dict],
    insert_database: bool = True,
) -> list[dict]:
    """
    This function anonymizes each the given rows with the
    RG Anonymizar.

    Parameters
    ----------
    src_client_db_path : str
        Connection URI of Client Database.

    table_name : str
        Table name where anonymization will be performed.

    columns_to_anonymize : list[str]
        Column names chosen for anonymization.

    rows_to_anonymize : list[dict]
        Rows provided for anonymization.

    insert_database : bool
        Flag to indicate if anonymized rows will be inserted or returned.

    Returns
    -------
    list[dict]
        Anonymized rows
    """

    # Get primary key column name of Client Database
    primary_key_name = get_primary_key(
        src_db_path=src_client_db_path, table_name=table_name
    )

    # Create table object of Client Database and
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_db_path=src_client_db_path, table_name=table_name
    )

    index_primary_key = get_index_column_table_object(
        table_object=table_client_db,
        column_name=primary_key_name,
    )

    # Run anonymization
    anonymized_rows = anonymization_data(
        primary_key_name=primary_key_name,
        rows_to_anonymize=rows_to_anonymize,
        columns_to_anonymize=columns_to_anonymize,
    )

    # Insert anonymized rows in database
    if insert_database:
        for anonymized_row in anonymized_rows:
            session_client_db.query(table_client_db).filter(
                table_client_db.c[index_primary_key]
                == anonymized_row[f"{primary_key_name}"]
            ).update({key: anonymized_row[key] for key in columns_to_anonymize})

        session_client_db.commit()

    session_client_db.close()

    return anonymized_rows


def anonymization_database(
    src_client_db_path: str, table_name: str, columns_to_anonymize: list[str]
) -> int:
    """
    This function anonymizes all database rows from all tables with the
    Email Anonymizer.

    Parameters
    ----------
    src_client_db_path : str
        Connection URI of Client Database

    table_name : str
        Table name where anonymization will be performed

    columns_to_anonymize : list[str]
        Column names chosen for anonymization.

    Returns
    -------
    int
        200 (Success code)
    """

    # Get primary key column name of Client Database
    primary_key_name = get_primary_key(
        src_db_path=src_client_db_path, table_name=table_name
    )

    # Create table object of Client Database and
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_db_path=src_client_db_path, table_name=table_name
    )

    index_primary_key = get_index_column_table_object(
        table_object=table_client_db,
        column_name=primary_key_name,
    )

    # Get rows on batch  for anonymization
    size = 1000
    statement = select(table_client_db)
    results_proxy = session_client_db.execute(
        statement  # Proxy to get rows on batch  for anonymization
    )
    results = results_proxy.fetchmany(size)  # Getting rows for anonymization

    while results:
        # Transform rows from tuples to dictionary
        rows_to_anonymize = [row._asdict() for row in results]

        # Run anonymization
        anonymized_rows = anonymization_data(
            primary_key_name=primary_key_name,
            rows_to_anonymize=rows_to_anonymize,
            columns_to_anonymize=columns_to_anonymize,
        )

        # Insert anonymized rows in database
        for anonymized_row in anonymized_rows:
            session_client_db.query(table_client_db).filter(
                table_client_db.c[index_primary_key]
                == anonymized_row[f"{primary_key_name}"]
            ).update({key: anonymized_row[key] for key in columns_to_anonymize})

        session_client_db.commit()

        # Getting rows for anonymization
        results = results_proxy.fetchmany(size)

    session_client_db.close()

    return 200
