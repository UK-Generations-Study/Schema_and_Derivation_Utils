# -*- coding: utf-8 -*-
"""
Created on Thu Sep 11 14:50:18 2025

@author: shegde
purpose: Set of functions used to get tumours from existing cancer summary
"""

import os
os.chdir('N:\CancerEpidem\BrBreakthrough\SHegde\Schema_and_Derivation_utils\CancerSummary\scripts')
import sys
sys.path.append(os.path.abspath('N:\CancerEpidem\BrBreakthrough\SHegde\Schema_and_Derivation_utils'))

import pandas as pd
import numpy as np
import Map_and_Derive_Stage as md
import config as cf


def prepare_legacy_data(ca_summary, ca_summary_schema, target_schema, logger, existing_casum):
    """
    Selects the best value for each field using field-specific or global source priority.
    Args:
        ca_summary (dataframe): legacy cancer sumamry dataset
        ca_summary_schema (dictionary): Schema for legacy cancer sumamry
        target_schema (string): Schema for the result dataset
        logger (logging): object to log the steps
        existing_casum (dataframe): existing cancer summary dataset
    Returns: 
        legacy_filtered (dataframe): legacy cancer sumamry dataset mapped and filtered for confirmed tumours
    """
    
    legacy_filtered = ca_summary[ca_summary['confirmed']=='1']

    # Run harmonization for legacy cancer summary
    legacy_mapped, mappings_used = md.harmonize_source(legacy_filtered, ca_summary_schema, target_schema,
                                                  cf.legacy_variables_to_map, logger, cf.legacy_special_rules)
    
    for src_var, info in mappings_used.items():    
        logger.info("Source column:" + str(src_var))
        logger.info("Rows mapped:" + str(info['changed_rows']))
        logger.info("Mapping dictionary used:" + str(info['mapping']))
    
    
    legacy_filtered = legacy_mapped.drop(['ER_STATUS', 'PR_STATUS', 'HER2_STATUS', 'SCREEN_DETECTED', 'LATERALITY'], axis=1).copy()
        
    legacy_filtered.rename(columns={'StudyID':'STUDY_ID', 'side':'LATERALITY', 'diagdate':'DIAGNOSIS_DATE',\
                                         'site_text':'CANCER_SITE', 'ICDt':'ICD_CODE', 'ICDm':'MORPH_CODE',\
                                         'er_Status': 'ER_STATUS', 'pr_Status': 'PR_STATUS', 'her2_Status': 'HER2_STATUS',\
                                         'grade':'GRADE', 'stage':'STAGE', 'T':'T_STAGE', 'N':'N_STAGE', 'M':'M_STAGE', \
                                         'nodes_tot':'NODES_TOTAL', 'nodes_pos':'NODES_POSITIVE',\
                                         'Tsize':'TUMOUR_SIZE', 'Screen_Detected':'SCREEN_DETECTED', \
                                         'LastUploadDate': 'CREATED_TIME', 'comments':'COMMENTS', 'Tnum': 'TUMOUR_ID',\
                                         'S_side':'S_LATERALITY', 'S_regdate':'S_DIAGNOSIS_DATE',\
                                         'S_ICDt':'S_ICD_CODE', 'S_ICDm':'S_MORPH_CODE',\
                                         'S_er_status': 'S_ER_STATUS', 'S_pr_status': 'S_PR_STATUS', 'S_her2_status': 'S_HER2_STATUS',\
                                         'S_ki67_status':'S_Ki67', 'S_grade':'S_GRADE', 'S_stage':'S_STAGE', 'S_T':'S_T_STAGE',\
                                         'S_N':'S_N_STAGE', 'S_M':'S_M_STAGE', 'S_nodes_tot':'S_NODES_TOTAL', 'S_nodes_pos':'S_NODES_POSITIVE',\
                                         'S_tsize':'S_TUMOUR_SIZE', 'S_ScreenDetect':'S_SCREEN_DETECTED', 'source':'S_STUDY_ID'}, inplace=True)

    legacy_filtered['GRADE'].replace('Low', 'GL', inplace=True)
    legacy_filtered['GRADE'].replace('low', 'GL', inplace=True)
    legacy_filtered['GRADE'].replace('High', 'GH', inplace=True)
    legacy_filtered['GRADE'].replace('high', 'GH', inplace=True)
    legacy_filtered['GRADE'].replace('Intermediate', 'GI', inplace=True)
    legacy_filtered['GRADE'].replace('intermediate', 'GI', inplace=True)
    
    legacy_filtered['GRADE'] = np.where(legacy_filtered['GRADE'].isna() | legacy_filtered['GRADE'].str.contains('G'), \
                                       legacy_filtered['GRADE'], 'G' + legacy_filtered['GRADE'])
    
    legacy_filtered['ICD_CODE'] = np.where(legacy_filtered['ICD_CODE'].notna() & legacy_filtered['ICD_CODE']=='C56Z',\
                                       legacy_filtered['ICD_CODE'].str[:3], legacy_filtered['ICD_CODE'])
        
    stage_map = {'I':'1', 'II':'2', 'III':'3', 'IV':'4'}
    legacy_filtered['STAGE'] = legacy_filtered['STAGE'].map(stage_map).fillna(legacy_filtered['STAGE'])
    
    legacy_filtered['MORPH_CODE'] = pd.to_numeric(legacy_filtered['MORPH_CODE'], errors='coerce').astype('Int64')
    
    cols_to_exclude = ['HER2_FISH', 'SCREENINGSTATUSCOSD_CODE', 'S_TUMOUR_ID', 'AGE_AT_DIAGNOSIS', 'Ki67', 'GROUPED_SITE']
    
    legacy_filtered = legacy_filtered[existing_casum.columns.difference(cols_to_exclude)]
    
    legacy_filtered['TUMOUR_ID'] = pd.to_numeric(legacy_filtered['TUMOUR_ID'], errors='coerce').astype('Int64')
    legacy_filtered['TUMOUR_SIZE'] =  pd.to_numeric(legacy_filtered['TUMOUR_SIZE'], errors='coerce')
    legacy_filtered['CREATED_TIME'] = pd.to_datetime(legacy_filtered['CREATED_TIME'], errors="coerce").dt.tz_localize(None)
    legacy_filtered['NODES_TOTAL'] = pd.to_numeric(legacy_filtered['NODES_TOTAL'], errors='coerce').astype('Int64')
    legacy_filtered['NODES_POSITIVE'] = pd.to_numeric(legacy_filtered['NODES_POSITIVE'], errors='coerce').astype('Int64')
    
    for col in legacy_filtered.columns:
        if col.startswith('S_') and col!='S_STUDY_ID':
            legacy_filtered[col] = np.where(legacy_filtered[col].notna(), legacy_filtered[col].str.split('\|\|').str[1], \
                           legacy_filtered[col])
            
    return legacy_filtered