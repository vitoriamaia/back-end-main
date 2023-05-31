import anonypy
import pandas as pd
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.orm import sessionmaker

from service.database_service import get_primary_key, create_table_session


def anonymization_database_rows(src_client_db_path, table_name, columns_to_anonymization, rows_to_anonymization):

    #Get primary key of Client Database
    primary_key = get_primary_key(src_client_db_path, table_name)

    # Get primary key of Client Database in columns_to_anonymization only query
    columns_to_anonymization.insert(0, primary_key)

    # Create table object of Client Database and 
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_db_path=src_client_db_path, 
        table_name=table_name, 
        columns_list=columns_to_anonymization
    )
    
    # Transform rows database to dataframe
    dataframe_to_anonymization = pd.DataFrame(
        data=rows_to_anonymization
    )
    dataframe_to_anonymization = dataframe_to_anonymization[columns_to_anonymization]
   
    # Remove primary key of columns_to_anonymization list
    # but save elements in save_primary_key_elements
    save_primary_key_elements = dataframe_to_anonymization[primary_key]
    dataframe_to_anonymization = dataframe_to_anonymization.drop(primary_key, axis=1)
    columns_to_anonymization.remove(primary_key)

    # Convert columns_to_anonymization to category
    for column in columns_to_anonymization:
        dataframe_to_anonymization[column] = dataframe_to_anonymization[column].astype("category")

    # Run anonymization
    anonymization_dataframe = pd.DataFrame()

    for column in columns_to_anonymization:

        from_anonymization_dataframe = []

        p = anonypy.Preserver(dataframe_to_anonymization, [str(column)], str(column))
        rows = p.anonymize_t_closeness(k=3, p=0.2)
        dataframe_aux = pd.DataFrame(rows)

        for row in range(0, len(dataframe_aux)):
            from_anonymization_dataframe = (
                from_anonymization_dataframe + [dataframe_aux[str(column)][row]] * dataframe_aux["count"][row]
            )

        anonymization_dataframe[str(column)] = from_anonymization_dataframe
    
    # Reorganize primary key elements
    anonymization_dataframe[f"{primary_key}"] = save_primary_key_elements

    # Get anonymized data to update
    dictionary_anonymized_data = anonymization_dataframe.to_dict(orient='records')

    # Update data
    for row_anonymized in dictionary_anonymized_data:
        session_client_db.query(table_client_db).\
        filter(table_client_db.c[0] == row_anonymized[f'{primary_key}']).\
        update(row_anonymized)

    session_client_db.commit()
    session_client_db.close()


def anonymization_database(src_client_db_path, table_name, columns_to_anonymization):

    # Get primary key of Client Database
    primary_key = get_primary_key(src_client_db_path, table_name)

    # Get primary key of Client Database in columns_to_anonymization only query
    columns_to_anonymization.insert(0, primary_key)

    # Create table object of Client Database and 
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_db_path=src_client_db_path, 
        table_name=table_name,
        columns_list=columns_to_anonymization
    )
        
    # Get data to dataframe
    from_dataframe = session_client_db.query(table_client_db).all()
    
    # Transform rows database to dataframe
    dataframe_to_anonymization = pd.DataFrame(
        data=from_dataframe
    )
    dataframe_to_anonymization = dataframe_to_anonymization[columns_to_anonymization]

    # Remove primary key of columns_to_anonymization list
    # but save elements in save_primary_key_elements
    save_primary_key_elements = dataframe_to_anonymization[primary_key]
    dataframe_to_anonymization = dataframe_to_anonymization.drop(primary_key, axis=1)
    columns_to_anonymization.remove(primary_key)

    # Convert columns_to_anonymization to category
    for column in columns_to_anonymization:
        dataframe_to_anonymization[column] = dataframe_to_anonymization[column].astype("category")
    
    print(dataframe_to_anonymization.iloc[0].to_dict())

    # Run anonymization
    anonymization_dataframe = pd.DataFrame()

    for column in columns_to_anonymization:

        from_anonymization_dataframe = []

        p = anonypy.Preserver(dataframe_to_anonymization, [str(column)], str(column))
        rows = p.anonymize_t_closeness(k=3, p=0.2)
        dfn = pd.DataFrame(rows)
        print(dfn)

        for row in range(0, len(dfn)):
            from_anonymization_dataframe = from_anonymization_dataframe + [dfn[str(column)][row]] * dfn["count"][row]

        anonymization_dataframe[str(column)] = from_anonymization_dataframe
    
    # Reorganize primary key elements
    anonymization_dataframe[f"{primary_key}"] = save_primary_key_elements

    # Get anonymized data to update
    dictionary_anonymized_data = anonymization_dataframe.to_dict(orient='records')

    # Update data
    for row_anonymized in dictionary_anonymized_data:
        session_client_db.query(table_client_db).\
        filter(table_client_db.c[0] == row_anonymized[f'{primary_key}']).\
        update(row_anonymized)

    session_client_db.commit()
    session_client_db.close()