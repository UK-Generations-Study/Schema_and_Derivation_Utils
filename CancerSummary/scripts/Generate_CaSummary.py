# -*- coding: utf-8 -*-
"""
Created on Tue Jul 1 10:20:27 2025

@author: shegde
purpose: Read and validate the Cancer Summary data sources
"""

#%% import required libraries and functions from utilities file
import os
os.chdir('N:\CancerEpidem\BrBreakthrough\SHegde\Schema_and_Derivation_utils\CancerSummary\scripts')
import sys
sys.path.append(os.path.abspath('N:\CancerEpidem\BrBreakthrough\SHegde\Schema_and_Derivation_utils'))

import json
import pandas as pd
from jsonschema import FormatChecker, Draft7Validator
from utilities import connect_DB, read_data, createLogger
import config as cf
import numpy as np

# create the logging object and format checker
logger = createLogger('CaSum_Validation', cf.Delivery_log_path)
formatcheck = FormatChecker()

#%% function to convert dataframe to dictionary with cleaning rules
def getJsonData(df):
    '''
    Returns a dictionary from a dataframe afterdata pre-processing
    
    Parameters:
        df (pandas dataframe): a pandas dataframe as input
    Returns:
        json_data (disct): dictionary to be returned
    '''
    # convert object to datetime
    for col in cf.casum_obj_to_dt:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # convert datetime columns into string with a format
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    # nan to null
    df = df.where(pd.notna(df), None)
    df = df.replace(np.nan, None)
    
    # convert fields to integer for validation
    for field in cf.casum_int_fields:
        if field in df.columns:
            df[field] = df[field].astype(int)
    
    # convert fields to string for validation
    for field in cf.casum_str_fields:
        if field in df.columns:
            df[field] = df[field].astype(str)

    json_data = df.to_dict('records')
    
    return json_data


# function to validate the data using JSON schema
def dataValidation(data_dict, schema):
    '''
    Returns a dictionary of invalid rows from the data
    
    Parameters:
        data_dict (dict): source data,
        schema (dict): JSON schema used ofr validation
    Returns:
        invalid rows (dict): dictionary to be returned with all the invalid data
    '''
    invalid_rows = {}

    #create the validator object to compile the schema
    validator = Draft7Validator(schema, format_checker=FormatChecker())

    for i,record in enumerate(data_dict):
        errors = list(validator.iter_errors(record))

    if errors:
        error_details = [[e.schema_path[1], e.validator_value, e.message] for e in errors]

        invalid_rows[i] = {"record": record, "errors": error_details}

    return invalid_rows

#%% create connection objects for databases
upload_conn = connect_DB('UpLoads', cf.live_server, logger)
nobkp_conn = connect_DB('NOBACKUP', cf.live_server, logger)

# read the data source for cancer summary
logger.info('Reading and Validate Cancer Summary source data')

dataframes = {}

for source, raw_schema in cf.casum_data_sources.items():
    
    # read the source data into dataframe
    if source=='CancerRegistry':
        data = pd.read_csv(os.path.join(cf.canreg_data_path, cf.canreg_fileanme))
        logger.info(source + ' row count: ' + str(len(data)))

    elif source in ['FlaggingCancers', 'FlaggingDeaths']:
        data = read_data('select * from ' + source + '', nobkp_conn, logger)
        logger.info(source + ' row count: ' + str(len(data)))

    else:
        data = read_data('select * from ' + source + '', upload_conn, logger)
        logger.info(source + ' row count: ' + str(len(data)))
        
    dataframes[source] = data.copy()

    # read the JSON schema
    with open(os.path.join(cf.casum_json_path, raw_schema), 'r') as schema:    
        json_schema = json.load(schema)

    # convert the dataframe to dictionary
    json_data = getJsonData(data.copy())
    
    logger.info('Validating ' + source)
    # validate teh data using schema
    invalid_rows = dataValidation(json_data, json_schema)
    
    if len(invalid_rows)!=0:
        logger.warning("Invalid data found during validation")
        sys.exit('Refer to Invalid rows')
    else:
        logger.info("Validation complete. No erros")

#%% combine registered data
fl_cancers = dataframes['FlaggingCancers']
hist_Brca = dataframes['Histopath_BrCa_GS_v1']
hist_Ovca = dataframes['OvCa_Histopath_II']
can_reg = dataframes['CancerRegistry']
ca_sum = dataframes['casummary_v1']

fl_cancers = fl_cancers[['StudyID', 'DCancer', 'CancerICD']]
hist_Brca = hist_Brca[['StudyID', 'Side', 'DiagDat', 'ReportDat', 'ER_Status', 'PR_Status', 'HER2_Status', 'CK56_Status', 'InvasiveGrade',\
                       'DCISGrade', 'Tstage', 'MStage', 'NStage', 'Type', 'AxillaryNodesTotal', 'ScreenDetected']]
    
hist_Ovca = hist_Ovca[['StudyID','ReportDat', 'DiagDat', 'PrimarySite']]