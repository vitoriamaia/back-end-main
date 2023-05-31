from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists

from controller import db
from model.anonymization_model import Anonymization, anonymizations_share_schema
from model.anonymization_type_model import AnonymizationType
from service.anonymization_types import (
    cpf_anonymizer_service,
    date_anonymizer_service,
    email_anonymizer_service,
    ip_anonymizer_service,
    named_entities_anonymizer_service,
    rg_anonymizer_service,
)
from service.database_service import get_cloud_database_path, get_database_path
from service.sse_service import generate_hash_column


def get_anonymizations() -> tuple[list[dict], int]:
    """
    This function returns the registered anonymizations.

    Parameters:
    ----------
        No parameters

    Returns:
    -------
    tuple[list[dict], int]
        (registered anonymizations, status code).
    """

    registered_anonymizations = anonymizations_share_schema.dump(
        Anonymization.query.all()
    )

    return (registered_anonymizations, 200)


def add_anonymization(
    id_db: int,
    id_anonymization_type: int,
    table_name: str,
    columns_to_anonymize: list[str],
) -> tuple[int, str]:
    """
    This function registers a anonymization.

    Parameters:
    ----------
    id_db : int
        Database ID.

    id_anonymization_type : int
        Database anonymization type ID.

    table_name : str
        Table name to anonymize.

    columns_to_anonymize : list[str]
        Columns names to anonymize.

    Returns:
    -------
    tuple[int, str]
        (status code, response message).
    """

    # Add anonymization
    anonymization = Anonymization(
        id_database=id_db,
        id_anonymization_type=id_anonymization_type,
        table=table_name,
        columns=columns_to_anonymize,
    )

    db.session.add(anonymization)
    db.session.commit()

    return (200, "anonymization_added")


def delete_anonymization(id_anonymization: int) -> tuple[int, str]:
    """
    This function deletes a anonymization.

    Parameters:
    ----------
    id_anonymization : int
        Anonymization ID.

    Returns:
    -------
    tuple[int, str]
        (status code, response message).
    """

    anonymization = Anonymization.query.filter_by(id=id_anonymization).first()

    if not anonymization:
        return (404, "anonymization_not_found")

    db.session.delete(anonymization)
    db.session.commit()

    anonymization = Anonymization.query.filter_by(id=id_anonymization).first()

    if not anonymization:
        return (200, "anonymization_deleted")

    return (400, "anonymization_not_deleted")


def anonymization_database_rows(
    id_db: int,
    table_name: str,
    rows_to_anonymization: list[dict],
    insert_database: bool,
) -> tuple:
    """
    This function anonymizes each the given rows according
    to pre-configured anonymizations.

    Parameters
    ----------
    id_db : int
        Database ID to be anonymized.

    table_name : str
        Table name where anonymization will be performed.

    rows_to_anonymization : list[dict]
        Rows provided for anonymization.

    insert_database : bool
        Flag to indicate if anonymized rows will be inserted or returned.

    Returns
    -------
    tuple
        If an error occurs, the return will be: (None, status code, response message).
        Else the return will be: (Anonymized rows, status code, response message).
    """

    # Check body request
    if (not id_db) or (not table_name) or (not rows_to_anonymization):
        return (None, 400, "anonymization_invalid_data")

    # Get client database path
    src_client_db_path = get_database_path(id_db)
    if not src_client_db_path:
        return (None, 404, "database_not_found")

    # Check connection with database found
    engine = create_engine(src_client_db_path)
    if not database_exists(engine.url):
        return (None, 409, "database_not_conected")

    # Get chosen columns
    lists_columns_anonymizations = anonymizations_share_schema.dump(
        Anonymization.query.filter_by(id_database=id_db, table=table_name).all()
    )
    if not lists_columns_anonymizations:
        return (None, 400, "anonymization_invalid_data")

    # Run anonymization for each anonymization types
    for anonymization in lists_columns_anonymizations:

        # Get anonymization type name by id
        anonymization_type_name = (
            AnonymizationType.query.filter_by(id=anonymization["id_anonymization_type"])
            .first()
            .name
        )

        if not anonymization["columns"]:
            continue

        # Run anonymization
        if anonymization_type_name == "named_entities_anonymizer":
            named_entities_anonymizer_service.anonymization_database_rows(
                src_client_db_path=src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymize=anonymization["columns"],
                rows_to_anonymize=rows_to_anonymization,
                insert_database=insert_database,
            )
            print("\n named_entities_anonymizer anonimizando\n")
            # print(rows_to_anonymization)

        elif anonymization_type_name == "date_anonymizer":
            date_anonymizer_service.anonymization_database_rows(
                src_client_db_path=src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymize=anonymization["columns"],
                rows_to_anonymize=rows_to_anonymization,
                insert_database=insert_database,
            )
            print("\n date_anonymizer anonimizando\n")
            # print(rows_to_anonymization)

        elif anonymization_type_name == "email_anonymizer":
            email_anonymizer_service.anonymization_database_rows(
                src_client_db_path=src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymize=anonymization["columns"],
                rows_to_anonymize=rows_to_anonymization,
                insert_database=insert_database,
            )
            print("\n email_anonymizer anonimizando\n")
            # print(rows_to_anonymization)

        elif anonymization_type_name == "ip_anonymizer":
            ip_anonymizer_service.anonymization_database_rows(
                src_client_db_path=src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymize=anonymization["columns"],
                rows_to_anonymize=rows_to_anonymization,
                insert_database=insert_database,
            )
            print("\n ip_anonymizer anonimizando\n")
            # print(rows_to_anonymization)

        elif anonymization_type_name == "rg_anonymizer":
            rg_anonymizer_service.anonymization_database_rows(
                src_client_db_path=src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymize=anonymization["columns"],
                rows_to_anonymize=rows_to_anonymization,
                insert_database=insert_database,
            )
            print("\n rg_anonymizer anonimizando\n")
            # print(rows_to_anonymization)

        elif anonymization_type_name == "cpf_anonymizer":
            cpf_anonymizer_service.anonymization_database_rows(
                src_client_db_path=src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymize=anonymization["columns"],
                rows_to_anonymize=rows_to_anonymization,
                insert_database=insert_database,
            )
            print("\n cpf_anonymizer anonimizando\n")
            # print(rows_to_anonymization)

        else:
            pass

    return (rows_to_anonymization, 200, "rows_database_anonymized")


def anonymization_table(src_client_db_path: str, id_db: int, table_name: str) -> int:
    """
    This function anonymizes all rows in a database table according
    to pre-configured anonymization.

    Parameters
    ----------
    src_client_db_path : str
        Connection URI of Client Database.

    id_db : int
        Database ID to be anonymized.

    table_name : str
        Table name where anonymization will be performed.

    Returns
    -------
    int
        status code.
    """

    # Get chosen columns
    lists_columns_anonymizations = anonymizations_share_schema.dump(
        Anonymization.query.filter_by(id_database=id_db, table=table_name).all()
    )
    if not lists_columns_anonymizations:
        return 400

    # Run anonymization for each anonymization types
    for anonymization in lists_columns_anonymizations:

        # Get anonymization type name by id
        anonymization_type_name = (
            AnonymizationType.query.filter_by(id=anonymization["id_anonymization_type"])
            .first()
            .name
        )

        if not anonymization["columns"]:
            continue

        # Run anonymization
        if anonymization_type_name == "named_entities_anonymizer":
            named_entities_anonymizer_service.anonymization_database(
                src_client_db_path=src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymize=anonymization["columns"],
            )
            print("\n\n name_entities_anonymizer anonimizando\n\n")

        elif anonymization_type_name == "date_anonymizer":
            date_anonymizer_service.anonymization_database(
                src_client_db_path=src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymize=anonymization["columns"],
            )
            print("\n\n date_anonymizer anonimizando\n\n")

        elif anonymization_type_name == "email_anonymizer":
            email_anonymizer_service.anonymization_database(
                src_client_db_path=src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymize=anonymization["columns"],
            )
            print("\n\n email_anonymizer anonimizando\n\n")

        elif anonymization_type_name == "ip_anonymizer":
            ip_anonymizer_service.anonymization_database(
                src_client_db_path=src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymize=anonymization["columns"],
            )
            print("\n\n ip_anonymizer anonimizando\n\n")

        elif anonymization_type_name == "rg_anonymizer":
            rg_anonymizer_service.anonymization_database(
                src_client_db_path=src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymize=anonymization["columns"],
            )
            print("\n\n rg_anonymizer anonimizando\n\n")

        elif anonymization_type_name == "cpf_anonymizer":
            cpf_anonymizer_service.anonymization_database(
                src_client_db_path=src_client_db_path,
                table_name=anonymization["table"],
                columns_to_anonymize=anonymization["columns"],
            )
            print("\n\n cpf_anonymizer anonimizando\n\n")

        else:
            return 400

    return 200


def anonymization_database(
    id_db_user: int, id_db: int, table_name: str
) -> tuple[int, str]:
    """
    This function anonymizes all database rows in each table according
    to pre-configured anonymizations.

    Parameters
    ----------
    id_db_user : int
        Database User ID

    id_db : int
        Database ID.

    table_name : str
        Table name to anonymize.

    Returns
    -------
    tuple[int, str]
        (status code, response message).
    """

    # Check request body
    if not id_db:
        return (400, "anonymization_invalid_data")

    # Get client database path
    src_client_db_path = get_database_path(id_db)

    if not src_client_db_path:
        return (404, "database_not_found")

    # Check connection with database found
    engine_client_db = create_engine(src_client_db_path)
    if not database_exists(engine_client_db.url):
        return (409, "database_not_conected")

    status_code = anonymization_table(
        src_client_db_path=src_client_db_path, id_db=id_db, table_name=table_name
    )

    if status_code == 400:
        return (400, "database_invalid_data")

    generate_hash_column(
        id_db_user=id_db_user,
        id_db=id_db,
        table_name=table_name,
    )

    return (200, "database_anonymized")
