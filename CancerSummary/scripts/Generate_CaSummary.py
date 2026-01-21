# -*- coding: utf-8 -*-
"""
Created on Tue Jul 1 10:20:27 2025

@author: shegde
purpose: Read, validate, map sources, and build the new Cancer Summary dataset
"""

#%% import required libraries and functions from utilities file
import os
os.chdir('N:\CancerEpidem\BrBreakthrough\SHegde\Schema_and_Derivation_utils\CancerSummary\scripts')
import sys
sys.path.append(os.path.abspath('N:\CancerEpidem\BrBreakthrough\SHegde\Schema_and_Derivation_utils'))

import json
import pandas as pd
from utilities import connect_DB, read_data, createLogger, write_to_DB
import Clean_and_Validate as cv
import Map_and_Derive_Stage as md
import Derive_Morph_Code as mc
import config as cf
import numpy as np
import Link_Tumours as lt
from datetime import datetime
import ICD_to_Site_Mapping as sm
import Get_Legacy_Tumours as gl
import Handle_Exceptions as he
import SummaryReports as sp

# create the logging object and format checker
logger = createLogger('Generate_Cancer_Summary', cf.Delivery_log_path)

#%% create connection objects for databases
upload_conn = connect_DB('UpLoads', cf.live_server, logger)
nobkp_conn = connect_DB('NOBACKUP', cf.live_server, logger)

# read the data source for cancer summary
logger.info('Reading and Validate Cancer Summary source data')

all_data = {}
all_schemas = {}

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
        all_schemas[source] = json_schema

    # convert the dataframe to dictionary
    json_data, cleaned_data = cv.getCleanJsonData(data.copy(), source)
    
    all_data[source] = cleaned_data
   
    logger.info('Validating ' + source)
    # validate teh data using schema
    invalid_rows = cv.dataValidation(json_data, json_schema)
    
    if len(invalid_rows)!=0:
        logger.warning("Invalid data found during validation")
        logger.info('Invalid entry count - ' + source + ': '+ str(len(invalid_rows)))
        # sys.exit('Refer to Invalid rows')
    else:
        logger.info("Validation complete. No erros")

#%% 
# get the updated dataframes
can_reg = all_data['CancerRegistry']
fl_cancers = all_data['FlaggingCancers']
hist_Brca = all_data['Histopath_BrCa_GS_v1']
hist_Ovca = all_data['OvCa_Histopath_II']
ca_summary = all_data['casummary_v1']
existing_casum = all_data['NewCancerSummary_v3']

# get the JSON schemas
can_reg_schema = all_schemas['CancerRegistry']
fl_cancers_schema = all_schemas['FlaggingCancers']
hist_Brca_schema = all_schemas['Histopath_BrCa_GS_v1']
hist_Ovca_schema = all_schemas['OvCa_Histopath_II']
ca_summary_schema = all_schemas['casummary_v1']
target_schema = all_schemas['NewCancerSummary_v3']

# select required columns
can_reg.rename(columns={'STUDY_ID':'PersonID'}, inplace=True)
registry = can_reg[['PersonID', 'TUMOURID', 'DIAGNOSISDATEBEST', 'SITE_ICD10_O2', 'HISTOLOGY_CODED', 'GRADE', \
                    'TUMOURSIZE', 'NODESEXCISED', 'NODESINVOLVED', 'LATERALITY', 'ER_STATUS', 'ER_SCORE', 'PR_STATUS', \
                    'PR_SCORE', 'HER2_STATUS','T_BEST', 'N_BEST', 'M_BEST', 'STAGE_BEST', 'SCREENDETECTED', \
                    'SCREENINGSTATUSCOSD_CODE']].copy()

flagging_cancers = fl_cancers[['StudyID', 'TumourID', 'DCancer', 'CancerICD', 'Histology']].copy()

brca = hist_Brca.copy()
ovca = hist_Ovca.copy()

# read Mailing for StudyID
mailing_conn = connect_DB('Mailing', cf.live_server, logger)

logger.info('Reading People table to map PersonID to StudyID and to get the Date of Birth')
people = read_data('select PersonID, StudyID, cast(DATEFROMPARTS(DOBYear, DOBMonth, DOBDay) as date)as DOB \
                   from People', mailing_conn, logger)

registry = registry.merge(people, on=['PersonID'], how='left').drop('PersonID', axis=1)

#%% perform the mapping on the dataframes
logger.info("Mapping source schema codes to Registry codes")

# Run harmonization for path breast source
brca_mapped, mappings_used = md.harmonize_source(brca, hist_Brca_schema, target_schema,
                                              cf.brca_variables_to_map, logger, cf.brca_special_rules)

for src_var, info in mappings_used.items():    
    logger.info("Source column:" + str(src_var))
    logger.info("Rows mapped:" + str(info['changed_rows']))
    logger.info("Mapping dictionary used:" + str(info['mapping']))
 
# Run harmonization for path ovarian source
ovca_mapped, mappings_used = md.harmonize_source(ovca, hist_Ovca_schema, target_schema,
                                              cf.ovca_variables_to_map, logger, cf.ovca_special_rules)

for src_var, info in mappings_used.items():    
    logger.info("Source column:" + str(src_var))
    logger.info("Rows mapped:" + str(info['changed_rows']))
    logger.info("Mapping dictionary used:" + str(info['mapping']))

brca_mapped = brca_mapped.drop(['ER_STATUS', 'PR_STATUS', 'HER2_STATUS', 'SCREEN_DETECTED', 'GRADE',\
                                'LATERALITY'], axis=1)

ovca_mapped = ovca_mapped.drop(['GRADE','STAGE'], axis=1)

#%% Pre-processing the source data to map and derive stage variable
brca_mapped['NStage'] = brca_mapped['NStage'].str.replace(r"\(.*?\)", "", regex=True).str.strip()

logger.info("Mapping T, N, and M Stage with a derived column for Stage derivation")
# Extract the value from Tstage to match with T_BEST from registry
brca_mapped['TstageMapped'] = np.where(brca_mapped['Tstage'].isin(['NA', 'X']),brca_mapped['Tstage'],\
                               brca_mapped['Tstage'].str.extract(r'(?i)T(.*)')[0])

brca_mapped['TstageDer'] = np.where(brca_mapped['Tstage'].isin(['NA', 'X']),brca_mapped['Tstage'],\
                               brca_mapped['Tstage'].str.extract(r'(?i)(T\d.?|T\D.*)')[0])

# Extract the value from NStage to match with N_BEST from registry
brca_mapped['NStageMapped'] = np.where(brca_mapped['NStage'].isin(['NA', 'X']),brca_mapped['NStage'],\
                               brca_mapped['NStage'].str.extract(r'(?i)N(.*)')[0])

brca_mapped['NStageDer'] = np.where(brca_mapped['NStage'].isin(['NA', 'X']),brca_mapped['NStage'],\
                               brca_mapped['NStage'].str\
                               .extract(r'(?i)(?:p)?(N[A-Za-z]?(?:\([^)]*\))*\s*\d+[A-Za-z]?|N[A-Za-z])')[0])

# Extract the value from MStage to match with M_BEST from registry
brca_mapped['MStageMapped'] = np.where(brca_mapped['MStage'].isin(['NA', 'X']),brca_mapped['MStage'],\
                               brca_mapped['MStage'].str.extract(r'(?i)M(.*)')[0])

brca_mapped['MStageDer'] = np.where(brca_mapped['MStage'].isin(['NA', 'X']),brca_mapped['MStage'],\
                               brca_mapped['MStage'].str.extract(r'(?i)(M\d.?|M\D.*)')[0])

brca_mapped['TstageDer'] = brca_mapped['TstageDer'].str.strip()
brca_mapped['NStageDer'] = brca_mapped['NStageDer'].str.strip()
brca_mapped['MStageDer'] = brca_mapped['MStageDer'].str.strip()

brca_mapped['TstageDer'] = brca_mapped['TstageDer'].str.replace("C", "c",)
brca_mapped['TstageDer'] = brca_mapped['TstageDer'].str.replace("m", "",)
brca_mapped['TstageDer'] = brca_mapped['TstageDer'].str.replace(r"\(.*?\)", "", regex=True).str.strip()
brca_mapped['TstageDer'] = brca_mapped['TstageDer'].str.replace("(", "",)

# Derive Stage variable
# --- Rules table ---
lookup_df = pd.DataFrame(cf.stage_rules)

# Convert to dictionary for fast lookup
lookup_dict = lookup_df.set_index("StagePattern").to_dict(orient="index")
patterns = lookup_df["StagePattern"].tolist()

logger.info("Deriving the Stage variable for HistoPath Breast data")
brca_mapped["Stage"] = brca_mapped.apply(md.get_stage, axis=1, args=(patterns, lookup_dict))

# Deriving ICD morphology code for Breast data
logger.info("Deriving ICD morphology code for breast cancer data")

brca_mapped['MORPH_CODE'] = brca_mapped.apply(mc.derive_breast_morphology_code, axis=1)
brca_mapped['MORPH_CODE'] = brca_mapped['MORPH_CODE'].str.replace("M", "",)

# derive ICD_CODE for breast pat report data
brca_mapped['ICD_CODE'] = np.where(brca_mapped['InvasiveCarcinoma']=='P', 'C50', \
                                   np.where((brca_mapped['InvasiveCarcinoma']=='N') & (brca_mapped['InsituCarcinoma']=='P'),\
                                            'D05', None))

#%% get source columns ready for tumour selection logic
logger.info("Prepare sources to link the tumours")

brca_mapped.drop(['Tstage', 'TstageDer', 'NStage', 'NStageDer', 'MStage', 'MStageDer'], axis=1, inplace=True)

registry.rename(columns={'StudyID':'STUDY_ID', 'DIAGNOSISDATEBEST': 'DIAGNOSIS_DATE',\
                         'HISTOLOGY_CODED': 'MORPH_CODE'}, inplace=True) 

brca_mapped.rename(columns={'StudyID':'STUDY_ID', 'Side': 'LATERALITY', 'DiagDat': 'DIAGNOSIS_DATE',\
                            'TstageMapped': 'Tstage', 'NStageMapped': 'NStage', 'MStageMapped': 'MStage'}, inplace=True) 

ovca_mapped.rename(columns={'StudyID':'STUDY_ID', 'DiagDat': 'DIAGNOSIS_DATE'}, inplace=True) 

flagging_cancers.rename(columns={'StudyID':'STUDY_ID', 'DCancer': 'DIAGNOSIS_DATE', 'Histology':'MORPH_CODE'}, inplace=True) 

registry_link = registry[['STUDY_ID', 'TUMOURID', 'DIAGNOSIS_DATE', 'SITE_ICD10_O2', 'MORPH_CODE', 'GRADE', \
                          'TUMOURSIZE', 'NODESEXCISED', 'NODESINVOLVED', 'LATERALITY', 'ER_STATUS', 'PR_STATUS', \
                          'HER2_STATUS', 'T_BEST', 'N_BEST', 'M_BEST', 'STAGE_BEST', 'SCREENDETECTED', \
                          'SCREENINGSTATUSCOSD_CODE']].copy()

brca_mapped['AxillaryNodesTotal'] = pd.to_numeric(brca_mapped['AxillaryNodesTotal'], errors='coerce').astype('Int64')
brca_mapped['OtherNodesTotal'] = pd.to_numeric(brca_mapped['OtherNodesTotal'], errors='coerce').astype('Int64')
brca_mapped['AxillaryNodesPositive'] = pd.to_numeric(brca_mapped['AxillaryNodesPositive'], errors='coerce').astype('Int64')
brca_mapped['OtherNodesPositive'] = pd.to_numeric(brca_mapped['OtherNodesPositive'], errors='coerce').astype('Int64')

brca_mapped['NodesTotal'] = brca_mapped['AxillaryNodesTotal'] + brca_mapped['OtherNodesTotal']
brca_mapped['NodesPositive'] = brca_mapped['AxillaryNodesPositive'] + brca_mapped['OtherNodesPositive']

#%% filter out the benign and non-malignant cases
logger.info("Filter source data as required")
brca_mapped = brca_mapped[~brca_mapped['MORPH_CODE'].str.startswith("0", na=False)]

# filter out non-malignant cases
brca_mapped = brca_mapped[brca_mapped['Malignant']=='Y']

brca_link = brca_mapped[['STUDY_ID', 'DIAGNOSIS_DATE', 'LATERALITY', 'MORPH_CODE', 'ICD_CODE',\
                       'InvasiveGrade', 'DCISGrade', 'SizeInvasiveTumour', 'SizeDCISOnly', 'NodesTotal',\
                       'NodesPositive', 'ER_Status', 'PR_Status', 'HER2_Status', 'HER2_FISH',\
                       'Tstage', 'MStage', 'NStage', 'Stage', 'ScreenDetected', 'Ki67', 'ReportCount', 'TumourCount']].copy()

brca_link['MORPH_CODE'] = pd.to_numeric(brca_link['MORPH_CODE'], errors='coerce').astype('Int64')

ovca_link = ovca_mapped[['STUDY_ID', 'DIAGNOSIS_DATE', 'Grade_I_II_III', 'Stage_FIGO']].copy()
ovca_link['Grade_I_II_III'] = np.where(ovca_link['Grade_I_II_III']=='N', None, ovca_link['Grade_I_II_III'])
ovca_link['ICD_CODE'] = 'C56'

#%%
logger.info("Convert ICD 8/9 version to ICD-10")

icd_code_mapping = pd.read_csv(os.path.join(cf.casum_report_path, cf.casum_ICD_conversion_file))

icd_code_mapping['ICD10_Code'] = icd_code_mapping['ICD10_Code'].astype(str).apply(lambda x:x[:4] if len(x)==5 else x)
icd_code_mapping['ICD9_Code'] = icd_code_mapping['ICD9_Code'].astype(str).apply(lambda x:x[:4] if len(x)==5 else x)

icd_mapping = dict(zip(icd_code_mapping['ICD9_Code'], icd_code_mapping['ICD10_Code']))

flagging_cancers['CancerICD'] = flagging_cancers['CancerICD'].str.rstrip('-')

flagging_cancers['CancerICD_mapped'] = flagging_cancers['CancerICD'].map(icd_mapping).fillna(flagging_cancers['CancerICD'])

flagging_cancers['CancerICD'] = np.where(~flagging_cancers['CancerICD'].str.match(r'^[A-Za-z]').fillna(False),\
                                   flagging_cancers['CancerICD_mapped'], flagging_cancers['CancerICD'])

flagging_cancers = flagging_cancers.drop(['CancerICD_mapped'], axis=1)

flagging_cancers_link = flagging_cancers[['STUDY_ID', 'TumourID', 'DIAGNOSIS_DATE', 'CancerICD', 'MORPH_CODE']].copy()
flagging_cancers_link['MORPH_CODE'] = pd.to_numeric(flagging_cancers_link['MORPH_CODE'], errors='coerce').astype('Int64')
flagging_cancers_link['ICD_CODE'] = np.where(flagging_cancers_link['CancerICD'].str.contains('C56|C50|D05'),\
                                             flagging_cancers_link['CancerICD'].str[:3],\
                                                 flagging_cancers_link['CancerICD'])

registry_link['STUDY_ID'] = registry_link['STUDY_ID'].astype('Int64')
registry_link['STAGE_BEST'] = np.where(registry_link['STAGE_BEST'].isin(['NA', 'U', 'X']), None, registry_link['STAGE_BEST'])
registry_link['MORPH_CODE'] = pd.to_numeric(registry_link['MORPH_CODE'], errors='coerce').astype('Int64')
registry_link['ICD_CODE'] = np.where(registry_link['SITE_ICD10_O2'].str.contains('C56|C50|D05'), registry_link['SITE_ICD10_O2'].str[:3],\
                                       registry_link['SITE_ICD10_O2'])

existing_casum['MORPH_CODE'] = pd.to_numeric(existing_casum['MORPH_CODE'], errors='coerce').astype('Int64')

# Legacy tumours (take only confirmed tumours)
legacy_filtered = gl.prepare_legacy_data(ca_summary, ca_summary_schema, target_schema, logger, existing_casum)

existing_casum['TUMOUR_ID'] = pd.to_numeric(existing_casum['TUMOUR_ID'], errors='coerce').astype('Int64')

#%% selecting data sources
data_sources = {
    "CancerRegistry": registry_link,
    "FlaggingCancers": flagging_cancers_link,
    "HistoPath_BrCa": brca_link,
    "HistoPath_OvCa": ovca_link,
#    "ExistingCaSum": existing_casum,
     "Legacy": legacy_filtered
}

for src, df in data_sources.items():
    if "LATERALITY" not in df.columns:
        df["LATERALITY"] = None
    if "MORPH_CODE" not in df.columns:
        df["MORPH_CODE"] = None
    if "DIAGNOSIS_DATE" not in df.columns:
        raise ValueError(f"{src} is missing DIAGNOSIS_DATE")
    df["DIAGNOSIS_DATE"] = pd.to_datetime(df["DIAGNOSIS_DATE"], errors="coerce").dt.tz_localize(None)

#%% Build Cancer Summary dataset
try:
    logger.info("Linking tumours across all data sources")
    
    records = []
    # Build clusters (tumours linked across sources)
    clusters = lt.build_clusters_optimized(data_sources, window=60)

except Exception as e:
    logger.error("Failed to link tumours:" + str(e))

try:
    logger.info("Building the dataset by source priority")
    # For each cluster, create ONE output row by selecting best values from all sources in the cluster
    for cluster in clusters:
        # Build cluster_matches dictionary: {source: row_data}
        cluster_matches = {}
        for entry in cluster:
            cluster_matches[entry["source"]] = entry["row"]
        
        # For this cluster, create one output row by selecting the best value 
        # for each field from all available sources in the cluster
        target_row = lt.select_value_per_field(cluster_matches, target_schema)
    
        records.append(target_row)
    
    CancerSummary = pd.DataFrame(records)

except Exception as e:
    
    logger.error("Failed to build tumour dataset:" + str(e))

# save the tumour source mapping
# lt.tumour_source_mapping(clusters, CancerSummary)

#%% Populate other remaining fields
logger.info("Deriving Age at diagnosis, Diagnosis Year and SITE")

CancerSummary['CREATED_TIME'] = datetime.now()
CancerSummary['CREATED_TIME'] = pd.to_datetime(CancerSummary['CREATED_TIME'], errors="coerce").dt.tz_localize(None)

CancerSummary['COMMENTS'] = None

# Derive age at diagnosis
CancerSummary = CancerSummary.merge(people, left_on=['STUDY_ID'], right_on=['StudyID'], how='left').drop(['StudyID', 'PersonID'], axis=1)
CancerSummary["DOB"] = pd.to_datetime(CancerSummary["DOB"], errors="coerce").dt.tz_localize(None)

CancerSummary['AGE_AT_DIAGNOSIS'] = np.where(CancerSummary['AGE_AT_DIAGNOSIS'].isna(), \
                                    ((CancerSummary['DIAGNOSIS_DATE'] - CancerSummary['DOB']).dt.days // 365.25),\
                                    CancerSummary['AGE_AT_DIAGNOSIS'])

CancerSummary['AGE_AT_DIAGNOSIS'] = pd.to_numeric(CancerSummary['AGE_AT_DIAGNOSIS'], errors='coerce').astype('Int64')

CancerSummary.drop('DOB', axis=1, inplace=True)

CancerSummary['DIAGNOSIS_YEAR'] = CancerSummary['DIAGNOSIS_DATE'].dt.year

logger.info("Convert ICD 8/9 version to ICD-10")

icd_code_mapping = pd.read_csv(os.path.join(cf.casum_report_path, cf.casum_ICD_conversion_file))

icd_code_mapping['ICD10_Code'] = icd_code_mapping['ICD10_Code'].astype(str).apply(lambda x:x[:4] if len(x)==5 else x)
icd_code_mapping['ICD9_Code'] = icd_code_mapping['ICD9_Code'].astype(str).apply(lambda x:x[:4] if len(x)==5 else x)

icd_mapping = dict(zip(icd_code_mapping['ICD9_Code'], icd_code_mapping['ICD10_Code']))

CancerSummary['ICD_CODE'] = CancerSummary['ICD_CODE'].str.rstrip('-')

CancerSummary['ICD_CODE_mapped'] = CancerSummary['ICD_CODE'].map(icd_mapping).fillna(CancerSummary['ICD_CODE'])

CancerSummary['ICD_CODE'] = np.where(~CancerSummary['ICD_CODE'].str.match(r'^[A-Za-z]').fillna(False),\
                                   CancerSummary['ICD_CODE_mapped'], CancerSummary['ICD_CODE'])

CancerSummary = CancerSummary.drop(['ICD_CODE_mapped'], axis=1)

# Populate SITE using ICD code
CancerSummary['CANCER_SITE'] = CancerSummary.apply(lambda row: sm.get_site_from_ICD(row['ICD_CODE'], row['S_STUDY_ID']), axis=1)

CancerSummary['GROUPED_SITE'] = CancerSummary['ICD_CODE'].apply(sm.group_sites)

total_count = len(CancerSummary)

logger.warning("Total count of tumours: " + str(len(CancerSummary)))

# filter out the benign cases
CancerSummary = CancerSummary[(CancerSummary['GROUPED_SITE']!='benign') & (CancerSummary['GROUPED_SITE']!='unknown')]

filtered_count = len(CancerSummary)

logger.warning("Count of benign and unknown tumour sites: " + str(total_count-filtered_count))
            
CancerSummary = CancerSummary[existing_casum.columns]

#%% Exceptional rules
# Ignore PHE_0125 as it is being processed above. Do not conisder from Legacy
CaSumFiltered_1 = CancerSummary[~CancerSummary['S_STUDY_ID'].str.contains('Deaths')]
logger.warning("Count of Flagging death tumour: " + str(len(CancerSummary) - len(CaSumFiltered_1)))

# Ignore rows with Invalid/NULL Diagnosis date
logger.info("Filter out invalid date of diagnosis")
CaSumFiltered_2 = CaSumFiltered_1[CaSumFiltered_1['AGE_AT_DIAGNOSIS'].notna()]
CaSumFiltered_3 = CaSumFiltered_2[CaSumFiltered_2['AGE_AT_DIAGNOSIS']>=0]
logger.warning("Count of invalid date of diagnosis: " + str(len(CaSumFiltered_1) - len(CaSumFiltered_3)))

# Replace unknown or Bilateral laterality using Path report
logger.info("Handling difference in LATERALITY between Registry and Path report")
CaSumFiltered_4 = he.expand_registry_laterality(CaSumFiltered_3)
CaSumFiltered_4 = CaSumFiltered_4[existing_casum.columns]
logger.warning("Count of tumours reduced after resolving LATERALITY between Registry and Path report: " + str(len(CaSumFiltered_3) - len(CaSumFiltered_4)))

#%%
logger.info("Standardise source variables and CANCER_SITE")
# Standardise the S_STUDY_ID and CANCER_SITE
for col in CaSumFiltered_4.columns:
    if col.startswith('S_'):
        CaSumFiltered_4 = CaSumFiltered_4.copy()
        CaSumFiltered_4[col] = CaSumFiltered_4[col].str.split('.').str[0]
        CaSumFiltered_4[col] = CaSumFiltered_4[col].str.replace(r'\bCancerRegistry\b', 'CancerRegistry_0125', regex=True)
        CaSumFiltered_4[col] = CaSumFiltered_4[col].str.replace(' ', '_')
        CaSumFiltered_4[col] = CaSumFiltered_4[col].str.replace('PHE_', 'CancerRegistry_')
        CaSumFiltered_4[col] = CaSumFiltered_4[col].str.replace('flagging', 'Flagging')
        CaSumFiltered_4[col] = CaSumFiltered_4[col].str.replace(r'\bFlagging\b', 'FlaggingCancers', regex=True)
        CaSumFiltered_4[col] = CaSumFiltered_4[col].str.replace('HistoPath_ovca', 'HistoPath_OvCa')
        CaSumFiltered_4[col] = CaSumFiltered_4[col].str.replace('Histopath_Report', 'HistoPath_BrCa')
        CaSumFiltered_4[col] = CaSumFiltered_4[col].str.replace('HistoPath_Report', 'HistoPath_BrCa')
        
#%% handle different Morphology code between Registry & Path Report
logger.info("Handling difference in ICD_CODE between sources")
CaSumFiltered_5, dropped_icd = he.resolve_icd_code_conflicts(CaSumFiltered_4, ["CancerRegistry_0125", "HistoPath_BrCa"])
logger.warning("Count of tumours reduced after resolving ICD_CODE between sources: " + str(len(CaSumFiltered_4) - len(CaSumFiltered_5)))

# handle different Morphology code between Registry & Flagging tumours
logger.info("Handling difference in MORPH_CODE between sources")
CaSumFiltered_6, dropped_mc = he.resolve_morph_code_conflicts(CaSumFiltered_5, ["CancerRegistry_0125", "FlaggingCancers"])
logger.warning("Count of tumours reduced after resolving MORPH_CODE between sources: " + str(len(CaSumFiltered_5) - len(CaSumFiltered_6)))

key_cols = ['STUDY_ID', 'DIAGNOSIS_DATE', 'ICD_CODE', 'MORPH_CODE', 'LATERALITY']

CaSumFiltered_7 = CaSumFiltered_6.loc[~(CaSumFiltered_6['TUMOUR_ID'].isna() & 
                                        CaSumFiltered_6.duplicated(subset=key_cols, keep=False))]
logger.info("Duplicate Count: " + str(len(CaSumFiltered_6) - len(CaSumFiltered_7)))

logger.info("Tumour dataset is ready. Final count:" + str(len(CaSumFiltered_6)))

#%% Summary reports
logger.info("Generating Summary reports")

CaSumFiltered_8 = CaSumFiltered_7.copy()

CaSumFiltered_8['S_STUDY_ID'] = CaSumFiltered_8['S_STUDY_ID'].str.lower().fillna('NaN')

CaSumFiltered_8['CANCER_SITE'] = CaSumFiltered_8['CANCER_SITE'].str.lower().fillna('NaN')

# execute the script for summary reports
sp.generate_summary_reports(CaSumFiltered_8, "SummaryReports.xlsx")

#%% Validate dataset with Schema
'''
logger.info("Validating the result data using JSON schema")
# type casting for schema validation
CaSumFiltered_7['TUMOUR_ID'] = pd.to_numeric(CaSumFiltered_7['TUMOUR_ID'], errors='coerce').astype('Int64')
CaSumFiltered_7['TUMOUR_SIZE'] =  pd.to_numeric(CaSumFiltered_7['TUMOUR_SIZE'], errors='coerce')
CaSumFiltered_7['MORPH_CODE'] = pd.to_numeric(CaSumFiltered_7['MORPH_CODE'], errors='coerce').astype('Int64')

final_json, cleaned_data = cv.getCleanJsonData(CaSumFiltered_7.copy(), "NewCancerSummary")

invalid_rows = cv.dataValidation(final_json, target_schema)

if len(invalid_rows)>=10:
    logger.warning("Invalid data found in Cancer Summary")
    # sys.exit('Refer to Invalid rows')
    logger.info('Invalid entry count: '+ str(len(invalid_rows)))

else:
    # Load the data to the database
    write_to_DB(CaSumFiltered_7, 'NewCancerSummary_v3', upload_conn, logger)
'''
#%% Pseudo-anonymise the data
logger.info("Pseudo-anonymise and create JSON data")

sidcode = read_data('select StudyID, TCode, Random from SIDCodes', mailing_conn, logger)
sidcode['StudyID'] = pd.to_numeric(sidcode['StudyID'], errors='coerce').astype('Int64')

CaSum_pseudo_anon = CaSumFiltered_7.merge(sidcode, left_on=['STUDY_ID'], right_on=['StudyID'], how='left')

CaSum_pseudo_anon['DIAGNOSIS_DATE'] = CaSum_pseudo_anon['DIAGNOSIS_DATE'] + pd.to_timedelta(CaSum_pseudo_anon['Random'], unit='days')

CaSum_pseudo_anon = CaSum_pseudo_anon.drop(['StudyID', 'STUDY_ID', 'Random'], axis=1)

CaSum_pseudo_anon['DIAGNOSIS_DATE'] = CaSum_pseudo_anon['DIAGNOSIS_DATE'].dt.strftime('%Y-%m-%d %H:%M:%S')
CaSum_pseudo_anon['CREATED_TIME'] = CaSum_pseudo_anon['CREATED_TIME'].dt.strftime('%Y-%m-%d %H:%M:%S')
CaSum_pseudo_anon = CaSum_pseudo_anon.replace(np.nan, None)

df_to_dict = CaSum_pseudo_anon.to_dict(orient='records')

json_data = {**cf.casum_version_ts, "data": df_to_dict}

with open(os.path.join(cf.casum_report_path, 
                       'CancerSummary_' + cf.casum_version_ts['version'] + '.json'), 'w') as f:
    json.dump(json_data, f, indent=4)