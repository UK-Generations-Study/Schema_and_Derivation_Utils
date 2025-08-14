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
    Returns a dictionary from a dataframe after data pre-processing
    
    Parameters:
        df (pandas dataframe): a pandas dataframe as input
        source(str): name of the data source
    Returns:
        json_data (dist): dictionary to be returned
    '''
    for col in df.columns:
        if col in cf.casum_StudyID:
            df.rename(columns={col:'StudyID'}, inplace=True)

    # dtype conversion for data sources except Cancer registry
    for col,dtype in cf.casum_convert_fields.items():
        if col in df.columns:
            if dtype=='date':
                df[col] = pd.to_datetime(df[col], errors='coerce')
            else:
                df[col] = df[col].astype(dtype)

    # convert datetime columns into string with a format
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

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

#%% 
can_reg = all_data['CancerRegistry']
fl_cancers = all_data['FlaggingCancers']
deaths = all_data['Deaths_GS']
hist_Brca = all_data['Histopath_BrCa_GS_v1']
hist_Ovca = all_data['OvCa_Histopath_II']
ca_summary = all_data['casummary_v1']

#%% data pre-processing
cols_to_update = ['LobularCarcinomaInsitu', 'PleomorphicLCIS', 'PagetsDisease', 'AxillaryNodesPresent', 'OtherNodesPresent']
condition = hist_Brca[cols_to_update] == 'Y'

hist_Brca[cols_to_update] = hist_Brca[cols_to_update].mask(condition, 'P')

#%% select required columns
can_reg.rename(columns={'STUDY_ID':'PersonID'}, inplace=True)
can_reg = can_reg[['PersonID', 'TUMOURID', 'DIAGNOSISDATEBEST', 'SITE_ICD10_O2', 'CODING_SYSTEM', 'CODING_SYSTEM_DESC',\
                    'MORPH_ICD10_O2', 'BEHAVIOUR_ICD10_O2', 'BEHAVIOUR_CODED_DESC', 'HISTOLOGY_CODED',
                    'HISTOLOGY_CODED_DESC', 'GRADE', 'TUMOURSIZE', 'NODESEXCISED', 'NODESINVOLVED', 'LATERALITY', \
                    'MULTIFOCAL', 'ER_STATUS', 'ER_SCORE', 'PR_STATUS', 'PR_SCORE', 'HER2_STATUS', 'NPI', 'DUKES', 'FIGO',\
                    'BRESLOW', 'CLARKS', 'T_PATH', 'N_PATH', 'M_PATH', 'STAGE_PATH', 'STAGE_PATH_PRETREATED', 'T_IMG',\
                    'N_IMG', 'M_IMG', 'STAGE_IMG', 'T_BEST', 'N_BEST', 'M_BEST', 'STAGE_BEST', 'SCREENDETECTED',\
                    'SCREENINGSTATUSCOSD_CODE', 'SCREENINGSTATUSCOSD_NAME', 'SCREENINGSTATUSFULL_CODE', \
                    'SCREENINGSTATUSFULL_NAME', 'CREG_CODE', 'CREG_NAME', 'DCO', 'EXCISIONMARGIN']].copy()

fl_cancers = fl_cancers[['StudyID', 'DCancer', 'CancerICD', 'Histology', 'TumourID']].copy()
fl_cancers.rename(columns={'Histology':'HistologyCode'}, inplace=True)

deaths = deaths[['StudyID', 'Source', 'Confirmed_death', 'DOD', 'UCCode', 'Reported_Cause']].copy()

hist_Brca = hist_Brca[['StudyID', 'Side', 'ReportDat', 'ER_Status', 'PR_Status', 'HER2_Status', 'CoreDat',\
                       'CK56_Status', 'DCISGrade', 'Tstage', 'MStage', 'NStage', 'Type', \
                       'ScreenDetected', 'ExcisionMargin', 'HER2_FISH', 'Ki67', 'ICDMorphologyCode',\
                       'Malignant', 'MitoticActivity', 'TumourExtent']].copy()

hist_Ovca = hist_Ovca[['StudyID','CoreDat', 'ReportDat', 'Primary_Site', 'Benign_Tumour', 'Borderline',\
                       'Epithelium', 'Grade_I_II_III', 'Stage_Best']].copy()

# read Mailing for StudyID
mailing_conn = connect_DB('Mailing', cf.live_server, logger)

logger.info('Reading People table for StudyID')
people = read_data('select PersonID, StudyID from People', mailing_conn, logger)
can_reg = can_reg.merge(people, on=['PersonID'], how='left').drop('PersonID', axis=1)

#%% combining the data sources
can_reg['dup_index'] = can_reg.groupby('StudyID').cumcount()
fl_cancers['dup_index'] = fl_cancers.groupby('StudyID').cumcount()
deaths['dup_index'] = deaths.groupby('StudyID').cumcount()
hist_Brca['dup_index'] = hist_Brca.groupby('StudyID').cumcount()
hist_Ovca['dup_index'] = hist_Ovca.groupby('StudyID').cumcount()

reg_with_fl_cancer = pd.merge(can_reg, fl_cancers, on=['StudyID', 'dup_index'], how='outer')

reg_with_flags = pd.merge(reg_with_fl_cancer, deaths, on=['StudyID', 'dup_index'], how='outer').drop('dup_index', axis=1)

hist_all = pd.merge(hist_Brca, hist_Ovca, on=['StudyID', 'dup_index'], how='outer').drop('dup_index', axis=1)