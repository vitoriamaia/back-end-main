import base64

import pandas as pd
import rsa
from sqlalchemy import create_engine, insert, select, update
from sqlalchemy_utils import create_database, database_exists

from model.database_key_model import DatabaseKey, database_key_share_schema
from service.database_service import (
    create_table_session,
    get_cloud_database_path,
    get_database_path,
    get_database_user_id,
    get_primary_key,
    get_sensitive_columns,
)


def generateKeys():
    # Generating keys
    (publicKey, privateKey) = rsa.newkeys(2048)

    # Save in PEM format
    publicKeyPEM = publicKey.save_pkcs1("PEM")
    privateKeyPEM = privateKey.save_pkcs1("PEM")

    # Transform from PEM to string base64
    publicKeyStr = str(base64.b64encode(publicKeyPEM))[2:-1]
    privateKeyStr = str(base64.b64encode(privateKeyPEM))[2:-1]

    return publicKeyStr, privateKeyStr


def loadKeys(publicKeyStr, privateKeyStr):
    # Transform from string base64 to Byte
    publicKeyByte = base64.b64decode(publicKeyStr.encode())
    privateKeyByte = base64.b64decode(privateKeyStr.encode())

    publicKey = rsa.PublicKey.load_pkcs1(publicKeyByte)
    privateKey = rsa.PrivateKey.load_pkcs1(privateKeyByte)

    return publicKey, privateKey


def encrypt(message, key):
    # Encrypt message
    encrypted_message = rsa.encrypt(message.encode(), key)

    # Convert from byte to base64
    base64_encrypted_message = base64.b64encode(encrypted_message)

    return str(base64_encrypted_message)[
        2:-1
    ]  # Remove caracter "b'" [0-1] and "'"[-1] of byte format.


def decrypt(encrypted_message, key):

    # Convert from string (base64) to bytes
    ciphertext = base64.b64decode(encrypted_message.encode())

    return rsa.decrypt(ciphertext, key).decode()


def encrypt_list(data_list, key):

    encrypted_list = []

    for data in data_list:
        data = str(data)
        encrypted_list.append(encrypt(data, key))

    return encrypted_list


def decrypt_list(data_list, key):

    decrypted_list = []

    for data in data_list:
        data = str(data)
        decrypted_list.append(decrypt(data, key))

    return decrypted_list


def encrypt_dict(data_dict, key):

    for keys in data_dict.keys():
        data_dict[keys] = encrypt(str(data_dict[keys]), key)

    return data_dict


def decrypt_dict(data_dict, key):

    for keys in data_dict.keys():
        data_dict[keys] = decrypt(data_dict[keys], key)

    return data_dict


def encrypt_database_rows(
    id_db_user: int,
    id_db: int,
    rows_to_encrypt: list[dict],
    table_name: str,
    update_database: int | bool,
) -> tuple[int, str]:
    """
    This function encrypts database rows.

    Parameters
    ----------
    id_db_user : int
        Database User ID

    id_db : int
        Database ID

    rows_to_encrypt : list[dict]
        Database rows to encrypt.

    table_name : str
        Table name to encrypt.

    update_database : int | bool
        Flag indicates if execute update database.

    Returns
    -------
    tuple[int, str]
        (status code, response message)
    """

    # Check user authorization
    if get_database_user_id(id_db=id_db) != id_db_user:
        return (401, "user_unauthorized")

    # Get client database path
    src_client_db_path = get_database_path(id_db=id_db)
    if not src_client_db_path:
        return (404, "database_not_found")

    # Create cloud database if not exist
    src_cloud_db_path = get_cloud_database_path(id_db=id_db)
    engine_cloud_db = create_engine(src_cloud_db_path)
    if not database_exists(engine_cloud_db.url):
        create_database(engine_cloud_db.url)

    # Get public and private keys of database
    result_keys = database_key_share_schema.dump(
        DatabaseKey.query.filter_by(id_db=id_db).first()
    )
    if not result_keys:
        return (404, "database_keys_not_found")

    # Load rsa keys
    publicKey, privateKey = loadKeys(
        result_keys["public_key"], result_keys["private_key"]
    )

    # Get columns to encrypt
    primary_key_name = get_primary_key(id_db=id_db, table_name=table_name)
    columns_list = [primary_key_name] + get_sensitive_columns(
        id_db_user=id_db_user, id_db=id_db, table_name=table_name
    )[0]

    # Create table object of Cloud Database and
    # session of Cloud Database to run sql operations
    table_cloud_db, session_cloud_db = create_table_session(
        src_db_path=src_cloud_db_path, table_name=table_name, columns_list=columns_list
    )

    if update_database:
        for row in rows_to_encrypt:
            row = {key: row[key] for key in columns_list}

            primary_key_value = row[primary_key_name]
            row.pop(primary_key_name, None)

            row = encrypt_dict(row, privateKey)

            statement = (
                update(table_cloud_db)
                .where(table_cloud_db.c[0] == primary_key_value)
                .values(row)
            )

            session_cloud_db.execute(statement)

        session_cloud_db.commit()
    else:
        for row in rows_to_encrypt:

            primary_key_value = row[primary_key_name]
            row.pop(primary_key_name, None)

            row = encrypt_dict(row, privateKey)

            row[primary_key_name] = primary_key_value

            statement = insert(table_cloud_db).values(row)

            session_cloud_db.execute(statement)

        session_cloud_db.commit()

    return (200, "rows_encrypted")


def encrypt_database(
    id_db_user: int,
    id_db: int,
    table_name: str,
) -> tuple[int, str]:
    """
    This function encrypt all rows of database .

    Parameters
    ----------
    id_db_user : int
        Database User ID

    id_db : int
        Database ID

    table_name : str
        Table name to encrypt.

    Returns
    -------
    tuple[int, str]
        (status code, response message)
    """

    # Check user authorization
    if get_database_user_id(id_db=id_db) != id_db_user:
        return (401, "user_unauthorized")

    # Get client database path
    src_client_db_path = get_database_path(id_db=id_db)
    if not src_client_db_path:
        return (404, "database_not_found")

    # Create cloud database if not exist
    src_cloud_db_path = get_cloud_database_path(id_db=id_db)
    engine_cloud_db = create_engine(src_cloud_db_path)
    if not database_exists(engine_cloud_db.url):
        create_database(engine_cloud_db.url)

    # Create table object of Client Database and
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        id_db=id_db, table_name=table_name
    )

    # Get columns to encrypt
    primary_key_name = get_primary_key(id_db=id_db, table_name=table_name)
    columns_list = [primary_key_name] + get_sensitive_columns(
        id_db_user=id_db_user, id_db=id_db, table_name=table_name
    )[0]

    # Get public and private keys of database
    result_keys = database_key_share_schema.dump(
        DatabaseKey.query.filter_by(id_db=id_db).first()
    )
    if not result_keys:
        return (404, "database_keys_not_found")

    # Load rsa keys
    publicKey, privateKey = loadKeys(
        result_keys["public_key"], result_keys["private_key"]
    )

    size_batch = 100
    statement = select(table_client_db)
    results_proxy = session_client_db.execute(statement)  # Proxy to get data on batch
    results = results_proxy.fetchmany(size_batch)  # Getting data

    # Encrypt database
    while results:

        from_db = [row._asdict() for row in results]

        # Create dataframe with data original database
        dataframe_db = pd.DataFrame(from_db, columns=columns_list)

        # Create name_columns without id
        names_columns = columns_list.copy()
        names_columns.remove(primary_key_name)

        # Encrypt each column
        for column in names_columns:
            dataframe_db[column] = encrypt_list(list(dataframe_db[column]), publicKey)

        # Add column hash
        dataframe_db["line_hash"] = [None] * len(dataframe_db)

        # Send data to database
        dataframe_db.to_sql(
            table_name, engine_cloud_db.connect(), if_exists="append", index=False
        )

        results = results_proxy.fetchmany(size_batch)  # Getting next data

    return (200, "database_encrypted")
