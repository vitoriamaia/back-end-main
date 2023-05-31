import numpy as np
import pandas as pd
import scipy.linalg as la
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.orm import sessionmaker

from service.database_service import get_primary_key, create_table_session


def anonymization_database_rows(src_client_db_path, table_name, columns_to_anonymization, rows_to_anonymization):

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

    # Run anonymization
    anonymization_dataframe = dataframe_to_anonymization[columns_to_anonymization].astype('int')
    anonymization_dataframe = anonymization_dataframe.to_numpy()
    anonymization_dataframe = anonymization_data(anonymization_dataframe)
    anonymization_dataframe = pd.DataFrame(data=anonymization_dataframe, columns=columns_to_anonymization)

    # Apply round function
    anonymization_dataframe.round(0)
    
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
        data=from_dataframe,
        columns=columns_to_anonymization
    )
    dataframe_to_anonymization = dataframe_to_anonymization[columns_to_anonymization]

    # Remove primary key of columns_to_anonymization list
    # but save elements in save_primary_key_elements
    save_primary_key_elements = dataframe_to_anonymization[primary_key]
    dataframe_to_anonymization = dataframe_to_anonymization.drop(primary_key, axis=1)
    columns_to_anonymization.remove(primary_key)

    # Run anonymization
    anonymization_dataframe = dataframe_to_anonymization[columns_to_anonymization].astype('int')
    anonymization_dataframe = anonymization_dataframe.to_numpy()
    anonymization_dataframe = anonymization_data(anonymization_dataframe)
    anonymization_dataframe = pd.DataFrame(data=anonymization_dataframe, columns=columns_to_anonymization)

    # Apply round function
    anonymization_dataframe.round(0)
    
    # Reorganize primary key elements
    anonymization_dataframe[f"{primary_key}"] = save_primary_key_elements

    # Get anonymized data to update
    dictionary_anonymized_data = anonymization_dataframe.to_dict(orient='records')

    # Update data
    for row_anonymized in dictionary_anonymized_data:
        session_client_db.query(table_client_db).\
        filter(table_client_db.c[0] == row_anonymized[f'{primary_key}']).\
        update(row_anonymized)

    # Commit changes
    session_client_db.commit()
    session_client_db.close()


def anonymization_data(data):
    # calculate the mean of each column
    mean = np.array(np.mean(data, axis=0).T)

    # center data
    data_centered = data - mean

    # calculate the covariance matrix
    cov_matrix = np.cov(data_centered, rowvar=False)

    # calculate the eignvalues and eignvectors
    evals, evecs = la.eigh(cov_matrix)

    # sort them
    idx = np.argsort(evals)[::-1]

    # Each columns of this matrix is an eingvector
    evecs = evecs[:, idx]
    evals = evals[idx]

    # explained variance
    variance_retained = np.cumsum(evals)/np.sum(evals)

    # calculate the transformed data
    data_transformed = np.dot(evecs.T, data_centered.T).T

    # randomize eignvectors
    new_evecs = evecs.copy().T
    for i in range(len(new_evecs)):
        np.random.shuffle(new_evecs[i])
    new_evecs = np.array(new_evecs).T

    # go back to the original dimension
    data_original_dimension = np.dot(data_transformed, new_evecs.T)
    data_original_dimension += mean

    return data_original_dimension


if __name__ == "__main__":
    src_client_db_path = "mysql://root:Dd16012018@localhost:3306/fake_db"
    table_name = 'nivel1'
    columns_to_anonymization = ["idade", "altura"]

    # Get primary key of Client Database
    primary_key = get_primary_key(src_client_db_path, table_name)

    # Create table object of Client Database and 
    # session of Client Database to run sql operations
    table_client_db, session_client_db = create_table_session(
        src_client_db_path, table_name
    )

    # Get data in database
    results_query = session_client_db.query(table_client_db).all()[:2]
    
    rows_to_anonymization = [row._asdict() for row in results_query]

    anonymization_database_rows(
        src_client_db_path,
        table_name,
        columns_to_anonymization,
        rows_to_anonymization
    )