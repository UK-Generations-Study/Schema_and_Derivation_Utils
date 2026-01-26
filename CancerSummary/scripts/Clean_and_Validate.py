# -*- coding: utf-8 -*-
"""
Created on Thu Sep 11 14:50:18 2025

@author: shegde
purpose: Set of functions used to clean and validate the source data
"""

import os
os.chdir('N:\CancerEpidem\BrBreakthrough\SHegde\Schema_and_Derivation_utils\CancerSummary\scripts')
import sys
sys.path.append(os.path.abspath('N:\CancerEpidem\BrBreakthrough\SHegde\Schema_and_Derivation_utils'))

import pandas as pd
from jsonschema import FormatChecker, Draft7Validator
import numpy as np
import config as cf

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
#    for field, new_val in cf.casum_clean_null_fields.items():
#        if field in df.columns:
#            df[field].fillna(new_val, inplace=True)

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