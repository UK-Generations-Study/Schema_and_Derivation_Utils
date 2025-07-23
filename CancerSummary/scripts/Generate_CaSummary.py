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

# Define rank mapping for HER2Score conversion
rank_map = {'0': 0, '1+': 1, '2+': 2, '3+': 3}

# Reverse mapping for output
reverse_rank_map = {v: k for k, v in rank_map.items()}

#%% convert HERScore to highest available marker
def getHighestMarker(value):
    '''
    Returns the highet value in the column
    
    Parameters:
        value (str): column values passed as parameter
    Returns:
        highest (str): highest HER2Score value available in the column value
    '''
    if pd.isna(value):
        return np.nan
    
    parts = value.split('/')
    
    def normalize_value(v):
        if v in ['1', '2', '3']:
            return f"{v}+"
        return v
    
    # Filter and normalize parts to valid ones
    normalized_parts = [normalize_value(p.strip()) for p in parts]
    valid_parts = [p for p in normalized_parts if p in rank_map]
    
    if not valid_parts:
        return value  # Leave as is if nothing is valid
    
    highest = max(valid_parts, key=lambda x: rank_map[x])
    return highest


# function to convert dataframe to dictionary with cleaning rules
def getCleanJsonData(df, source):
    '''
    Returns a dictionary from a dataframe afterdata pre-processing
    
    Parameters:
        df (pandas dataframe): a pandas dataframe as input
        source(str): name of the data source
    Returns:
        json_data (disct): dictionary to be returned
    ''' 
    # dtype conversion for data sources except Cancer registry
    for col,dtype in cf.casum_convert_fields.items():
        if col in df.columns:
            if dtype=='date':
                df[col] = pd.to_datetime(df[col], errors='coerce')
            else:
                df[col] = df[col].astype(dtype)

    # convert datetime columns into string with a format and same name for all StudyID fields
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        elif col in cf.casum_StudyID:
            df.rename(columns={col:'StudyID'}, inplace=True)

    # clean and pre-process the null values in raw data
    for field, new_val in cf.casum_clean_null_fields.items():
        if field in df.columns:
            df[field].fillna(new_val, inplace=True)

    # change the cases to match the schema
    if source=='FlaggingCancers':
        df['Region'] = df['Region'].replace("England And Wales", "England and Wales")
    
    if source=='FlaggingDeaths' or source=='Deaths_GS':
        df['Region'] = df['Region'].replace("England And Wales", "England and Wales")
        df['Region'] = df['Region'].replace("NI", "Northern Ireland")
        df['Region'] = df['Region'].replace("Death Certificate",  None)
        df['Region'] = df['Region'].replace("",  None)
        
    if source=='Histopath_BrCa_GS_v1':
        df['Side'] = df['Side'].replace("l","L")
        df['AxillaryNodesPresent'] = df['AxillaryNodesPresent'].replace("y", "Y",)
        df['InvasiveGrade'] = df['InvasiveGrade'].replace("1 ", "1")
        df['PR_Status'] = df['PR_Status'].replace("p", "P")
        
        # update HER2Score to higher end value
        df['HER2_Score'] = df['HER2_Score'].apply(getHighestMarker)            

    if source=='CancerRegistry':
        for col,dtype in cf.casum_canreg_dtype_updates.items():
            if col in ['SCREENINGSTATUSFULL_CODE', 'BRESLOW']:
                df[col] = df[col].astype(str)
            else:    
                df[col] = df[col].apply(
                    lambda x: str(int(x)) if pd.notna(x) and x==int(x) else (str(x) if pd.notna(x) else np.nan))        

    # nan to null
    df = df.where(pd.notna(df), None)
    df = df.replace(np.nan, None)
    
    # convert the data into JSON format
    json_data = df.to_dict(orient='records')
    
    return json_data, df


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

all_data = {}

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

    # read the JSON schema
    with open(os.path.join(cf.casum_json_path, raw_schema), 'r') as schema:    
        json_schema = json.load(schema)

    # convert the dataframe to dictionary
    json_data, cleaned_data = getCleanJsonData(data.copy(), source)
    
    all_data[source] = cleaned_data
   
    logger.info('Validating ' + source)
    # validate teh data using schema
    invalid_rows = dataValidation(json_data, json_schema)
    
    if len(invalid_rows)!=0:
        logger.warning("Invalid data found during validation")
        # sys.exit('Refer to Invalid rows')
        logger.info('Invalid entry count - ' + source + ': '+ str(len(invalid_rows)))
    else:
        logger.info("Validation complete. No erros")

#%% combine registered data
# fl_cancers = all_data['FlaggingCancers']
# hist_Brca = all_data['Histopath_BrCa_GS_v1']
# hist_Ovca = all_data['OvCa_Histopath_II']
# can_reg = all_data['CancerRegistry']
# ca_sum = all_data['casummary_v1']

# fl_cancers = fl_cancers[['StudyID', 'DCancer', 'CancerICD']]
# hist_Brca = hist_Brca[['StudyID', 'Side', 'DiagDat', 'ReportDat', 'ER_Status', 'PR_Status', 'HER2_Status', 'CK56_Status', 'InvasiveGrade',\
#                        'DCISGrade', 'Tstage', 'MStage', 'NStage', 'Type', 'AxillaryNodesTotal', 'ScreenDetected']]
    
# hist_Ovca = hist_Ovca[['StudyID','ReportDat', 'DiagDat', 'Primary_Site']]