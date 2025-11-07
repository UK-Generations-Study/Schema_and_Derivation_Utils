import config as cf
import pandas as pd
from sqlalchemy import create_engine
import urllib
import sys
import pyodbc
from datetime import datetime
import os
import logging


def connect_DB(db_name, server, logger):
    '''
    Returns the connection object to the database
    
    Parameters:
        db_name (string): database name to be connected
        server (string): database server to connect
        logger (logging): logger object to log the statements
        
    Returns:
        engine (object): connection object to the specified database
    '''
    try:
        # connection string for SQL Server database
        params = urllib.parse.quote_plus(f'DRIVER={cf.driver_64};SERVER={server};DATABASE={db_name};Trusted_connection=yes')
        url = f'mssql+pyodbc:///?odbc_connect={params}'
        engine = create_engine(url)
        
        return engine
    
    except Exception as e:
        logger.error('Failed to connect to the database' + str(e))
        sys.exit(1)



def createLogger(name, path):
    '''
    Creates the logging object to store each log statement in specified format
    Parameters:
        name (string): Name of the process to log
        path (string): path for saving the log file
    Returns:
        logger (logging object): object used in the main script to log the statements
    '''  
    # set the logging object to required configuration
    timestamp = datetime.now().strftime('%Y-%m-%d_%H_%M_%S')
    log_filename = name + '_Log_' + timestamp + '.log'
    log_location = os.path.join(path, log_filename)
    
    logger = logging.getLogger('QuestionnaireETL')
    logger.setLevel(logging.INFO)
    
    # add location for log file to be saved
    file_handler = logging.FileHandler(log_location)
    file_handler.setLevel(logging.INFO)
    
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    
    # format the log message
    formatter = logging.Formatter("%(asctime)s - %(levelname)s: %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)
        
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    
    return logger



def read_data(query, conn, logger):
    '''
    Returns a dataframe
    
    Parameters:
        query (string): query to run to read data from SQL Server
        conn (object): connection object for the specific databse to be queried
        logger (logging): logger object to log the statements
    Returns:
        df (pandas dataframe): dataframe containing the results of the query
    '''    
    import traceback
    try:
        df = pd.read_sql(query, conn)
        
        return df
    
    except Exception as e:
        logger.error('Failed to execute the query' + str(e))
        logger.error(traceback.format_exec())
        sys.exit(1)


