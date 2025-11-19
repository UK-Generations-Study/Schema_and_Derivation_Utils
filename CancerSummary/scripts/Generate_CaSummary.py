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
import matplotlib.pyplot as plt

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
#        sys.exit('Refer to Invalid rows')
    else:
        logger.info("Validation complete. No erros")

#%% 
# get the updated dataframes
can_reg = all_data['CancerRegistry']
fl_cancers = all_data['FlaggingCancers']
hist_Brca = all_data['Histopath_BrCa_GS_v1']
hist_Ovca = all_data['OvCa_Histopath_II']
ca_summary = all_data['casummary_v1']
existing_casum = all_data['NewCancerSummary']
existing_casum = existing_casum.drop(['SUMMARY_ID'], axis=1)

# get the JSON schemas
can_reg_schema = all_schemas['CancerRegistry']
fl_cancers_schema = all_schemas['FlaggingCancers']
hist_Brca_schema = all_schemas['Histopath_BrCa_GS_v1']
hist_Ovca_schema = all_schemas['OvCa_Histopath_II']
ca_summary_schema = all_schemas['casummary_v1']
target_schema = all_schemas['NewCancerSummary']

# select required columns
can_reg.rename(columns={'STUDY_ID':'PersonID'}, inplace=True)
registry = can_reg[['PersonID', 'TUMOURID', 'DIAGNOSISDATEBEST', 'SITE_ICD10_O2', 'MORPH_ICD10_O2',\
                    'MORPH_CODED', 'HISTOLOGY_CODED', 'GRADE', 'TUMOURSIZE', 'NODESEXCISED', \
                    'NODESINVOLVED', 'LATERALITY', 'ER_STATUS', 'ER_SCORE', 'PR_STATUS', 'PR_SCORE', 'HER2_STATUS',\
                    'T_BEST', 'N_BEST', 'M_BEST', 'STAGE_BEST', 'SCREENDETECTED', 'SCREENINGSTATUSCOSD_CODE']].copy()

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

# Deriving ICD morphology code for Path report data


# Deriving ICD morphology code for Breast data
logger.info("Deriving ICD morphology code for breast cancer data")

brca_mapped['MORPH_CODE'] = brca_mapped.apply(mc.derive_breast_morphology_code, axis=1)
brca_mapped['MORPH_CODE'] = brca_mapped['MORPH_CODE'].str.replace("M", "",)

logger.info("Deriving ICD morphology code for ovarian cancer data")
ovca_mapped['MORPH_CODE'] = ovca_mapped.apply(mc.derive_ovarian_morphology_code, axis=1)

#%% get source columns ready for tumour selection logic
logger.info("Prepare sources to link the tumours")

brca_mapped.drop(['Tstage', 'TstageDer', 'NStage', 'NStageDer', 'MStage', 'MStageDer'], axis=1, inplace=True)

registry.rename(columns={'StudyID':'STUDY_ID', 'DIAGNOSISDATEBEST': 'DIAGNOSIS_DATE',\
                         'HISTOLOGY_CODED': 'MORPH_CODE'}, inplace=True) 

brca_mapped.rename(columns={'StudyID':'STUDY_ID', 'Side': 'LATERALITY', 'DiagDat': 'DIAGNOSIS_DATE',\
                            'TstageMapped': 'Tstage', 'NStageMapped': 'NStage', 'MStageMapped': 'MStage'}, inplace=True) 

ovca_mapped.rename(columns={'StudyID':'STUDY_ID', 'DiagDat': 'DIAGNOSIS_DATE'}, inplace=True) 

flagging_cancers.rename(columns={'StudyID':'STUDY_ID', 'DCancer': 'DIAGNOSIS_DATE', 'Histology':'MORPH_CODE'}, inplace=True) 

#flagging_deaths.rename(columns={'StudyID':'STUDY_ID', 'DDeath': 'DIAGNOSIS_DATE'}, inplace=True) 

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

#%% # filter out the benign and non-malignant cases
logger.info("Filter source data as required")
brca_mapped = brca_mapped[~brca_mapped['MORPH_CODE'].str.startswith("0", na=False)]

# filter out non-malignant cases
brca_mapped = brca_mapped[brca_mapped['Malignant']=='Y']

brca_link = brca_mapped[['STUDY_ID', 'DIAGNOSIS_DATE', 'LATERALITY', 'MORPH_CODE',\
                       'InvasiveGrade', 'DCISGrade', 'SizeInvasiveTumour', 'SizeDCISOnly', 'NodesTotal',\
                       'NodesPositive', 'ER_Status', 'PR_Status', 'HER2_Status', 'HER2_FISH',\
                       'Tstage', 'MStage', 'NStage', 'Stage', 'ScreenDetected', 'Ki67']].copy()

brca_link['MORPH_CODE'] = pd.to_numeric(brca_link['MORPH_CODE'], errors='coerce').astype('Int64')

ovca_link = ovca_mapped[['STUDY_ID', 'DIAGNOSIS_DATE', 'MORPH_CODE', 'Grade_I_II_III', 'Stage_FIGO']].copy()
ovca_link['MORPH_CODE'] = pd.to_numeric(ovca_link['MORPH_CODE'], errors='coerce').astype('Int64')
ovca_link['Grade_I_II_III'] = np.where(ovca_link['Grade_I_II_III']=='N', None, ovca_link['Grade_I_II_III'])

flagging_cancers_link = flagging_cancers[['STUDY_ID', 'TumourID', 'DIAGNOSIS_DATE', 'CancerICD', 'MORPH_CODE']].copy()
flagging_cancers_link['MORPH_CODE'] = pd.to_numeric(flagging_cancers_link['MORPH_CODE'], errors='coerce').astype('Int64')

registry_link['STUDY_ID'] = registry_link['STUDY_ID'].astype('Int64')
registry_link['STAGE_BEST'] = np.where(registry_link['STAGE_BEST'].isin(['NA', 'U', 'X']), None, registry_link['STAGE_BEST'])
registry_link['MORPH_CODE'] = pd.to_numeric(registry_link['MORPH_CODE'], errors='coerce').astype('Int64')

existing_casum['MORPH_CODE'] = pd.to_numeric(existing_casum['MORPH_CODE'], errors='coerce').astype('Int64')

# Legacy tumours (take only confirmed tumours)
legacy_filtered = gl.prepare_legacy_data(ca_summary, ca_summary_schema, target_schema, logger, existing_casum)

# selecting data sources
data_sources = {
    "CancerRegistry": registry_link.copy(),
    "FlaggingCancers": flagging_cancers_link.copy(),
    "HistoPath_BrCa": brca_link.copy(),
    "HistoPath_OvCa": ovca_link.copy(),
    "ExistingCaSum": existing_casum.copy(),
    "Legacy": legacy_filtered.copy()
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
    clusters = lt.build_clusters_optimized(data_sources, window=90)

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
#lt.tumour_source_mapping(clusters, CancerSummary)

#%% Populate other remaining fields
logger.info("Deriving Age at diagnosis and SITE")

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

logger.info("Convert ICD 8/9 version to ICD-10")

icd_code_mapping = pd.read_csv(os.path.join(cf.casum_report_path, cf.casum_ICD_conversion_file))

icd_code_mapping['ICD10_Code'] = icd_code_mapping['ICD10_Code'].astype(str).apply(lambda x:x[:4] if len(x)==5 else x)
icd_code_mapping['ICD9_Code'] = icd_code_mapping['ICD9_Code'].astype(str).apply(lambda x:x[:4] if len(x)==5 else x)

icd_mapping = dict(zip(icd_code_mapping['ICD9_Code'], icd_code_mapping['ICD10_Code']))

CancerSummary['ICD_CODE'] = CancerSummary['ICD_CODE'].str.rstrip('-')

CancerSummary['ICD_CODE_mapped'] = CancerSummary['ICD_CODE'].map(icd_mapping).fillna(CancerSummary['ICD_CODE'])

CancerSummary['ICD_CODE'] = np.where(~CancerSummary['ICD_CODE'].str.match(r'^[A-Za-z]').fillna(False) & CancerSummary['ICD_CODE'].notna(),\
                                   CancerSummary['ICD_CODE_mapped'], CancerSummary['ICD_CODE'])\

CancerSummary = CancerSummary.drop(['ICD_CODE_mapped'], axis=1)

# Populate SITE using ICD code
CancerSummary['CANCER_SITE'] = CancerSummary.apply(lambda row: sm.get_site_from_ICD(row['ICD_CODE'], row['S_STUDY_ID']), axis=1)

CancerSummary = CancerSummary[existing_casum.columns]

#%% Exceptional rules
# Ignore PHE_0125 as it is being processed above. Do not conisder from Legacy
CaSumFiltered_1 = CancerSummary[~CancerSummary['S_STUDY_ID'].str.contains('PHE_0125|Deaths')]

logger.info("Standardise source variables and CANCER_SITE")
# Standardise the S_STUDY_ID and CANCER_SITE
for col in CaSumFiltered_1.columns:
    if col.startswith('S_'):
        CaSumFiltered_1 = CaSumFiltered_1.copy()
        CaSumFiltered_1[col] = CaSumFiltered_1[col].str.split('.').str[0]
        CaSumFiltered_1[col] = CaSumFiltered_1[col].str.replace('CancerRegistry', 'CancerRegistry_0125')
        CaSumFiltered_1[col] = CaSumFiltered_1[col].str.replace(' ', '_')
        CaSumFiltered_1[col] = CaSumFiltered_1[col].str.replace('PHE_', 'CancerRegistry_')
        CaSumFiltered_1[col] = CaSumFiltered_1[col].str.replace('flagging', 'Flagging')
        CaSumFiltered_1[col] = CaSumFiltered_1[col].str.replace(r'\bFlagging\b', 'FlaggingCancers', regex=True)
        CaSumFiltered_1[col] = CaSumFiltered_1[col].str.replace('HistoPath_ovca', 'HistoPath_OvCa')
        CaSumFiltered_1[col] = CaSumFiltered_1[col].str.replace('Histopath_Report', 'HistoPath_BrCa')
        CaSumFiltered_1[col] = CaSumFiltered_1[col].str.replace('HistoPath_Report', 'HistoPath_BrCa')
        
#%% Ignore rows with Invalid/NULL Diagnosis date
logger.info("Filter out invalid date of diagnosis")
CaSumFiltered_2 = CaSumFiltered_1[CaSumFiltered_1['AGE_AT_DIAGNOSIS'].notna()]
CaSumFiltered_3 = CaSumFiltered_2[CaSumFiltered_2['AGE_AT_DIAGNOSIS']>=0]

# Replace unknown or Bilateral laterality using Path report
logger.info("Handling difference in LATERALITY between Registry and Path report")
CaSumFiltered_4 = he.expand_registry_laterality(CaSumFiltered_3)
CaSumFiltered_4 = CaSumFiltered_4[existing_casum.columns]

# handle different Morphology code between Registry & Path Report
logger.info("Handling difference in MORPH_CODE between Registry and Path report")
CaSumFiltered_5 = he.resolve_morph_code_conflicts(CaSumFiltered_4, ["CancerRegistry_0125", "HistoPath_BrCa"])

# handle different Morphology code between Registry & Flagging tumours
logger.info("Handling difference in MORPH_CODE between Registry and Flagging")
CaSumFiltered_6 = he.resolve_morph_code_conflicts(CaSumFiltered_5, ["CancerRegistry_0125", "FlaggingCancers"]) 

logger.info("Tumour dataset is ready")

#%% Validate dataset with Schema
logger.info("Validating the result data using JSON schema")

# type casting for schema validation
CaSumFiltered_6['TUMOUR_ID'] = pd.to_numeric(CaSumFiltered_6['TUMOUR_ID'], errors='coerce').astype('Int64')
CaSumFiltered_6['TUMOUR_SIZE'] =  pd.to_numeric(CaSumFiltered_6['TUMOUR_SIZE'], errors='coerce')

final_json, cleaned_data = cv.getCleanJsonData(CaSumFiltered_6.copy(), "NewCancerSummary")

invalid_rows = cv.dataValidation(final_json, target_schema)

if len(invalid_rows)!=0:
    logger.warning("Invalid data found in Cancer Summary")
    # sys.exit('Refer to Invalid rows')
    logger.info('Invalid entry count: '+ str(len(invalid_rows)))

#else:
    # Load the data to the database
#    write_to_DB(CancerSummary, 'NewCancerSummary', upload_conn, logger)

#%% Summary reports
logger.info("Generating Summary reports")

summary_path = "SummaryReports_v2.xlsx"

# QC checks
invalid_diagdate = CancerSummary[CancerSummary['DIAGNOSIS_DATE'].isna() | (CancerSummary['AGE_AT_DIAGNOSIS']<0)]
logger.info("Invalid DIAGNOSIS_DATE count: " + str(len(invalid_diagdate)))

unknown_sites = CancerSummary[CancerSummary['CANCER_SITE'].isna()][['S_STUDY_ID', 'ICD_CODE', 'MORPH_CODE', 'CANCER_SITE']]
logger.info("Unknown CANCER_SITE count: " + str(len(unknown_sites)))

CaSumFiltered_7 = CaSumFiltered_6.copy()

legacy_filtered['S_STUDY_ID'] = legacy_filtered['S_STUDY_ID'].str.lower().fillna('NaN')
CaSumFiltered_7['S_STUDY_ID'] = CaSumFiltered_7['S_STUDY_ID'].str.lower().fillna('NaN')

legacy_filtered['CANCER_SITE'] = legacy_filtered['CANCER_SITE'].str.lower().fillna('NaN')
CaSumFiltered_7['CANCER_SITE'] = CaSumFiltered_7['CANCER_SITE'].str.lower().fillna('NaN')

#%% Completeness of data - overall
bins = [0, 2004, 2009, 2014, 2019, 2025]
labels = ['<2004', '2005-2009', '2010-2014', '2015-2019', '2020-2025']
CaSumFiltered_7['YEAR'] = CaSumFiltered_7['DIAGNOSIS_DATE'].dt.year
CaSumFiltered_7['year_range'] = pd.cut(CaSumFiltered_7['YEAR'], bins=bins, labels=labels)

all_cancer_cols = ['STUDY_ID', 'TUMOUR_ID', 'DIAGNOSIS_DATE', 'AGE_AT_DIAGNOSIS', 'ICD_CODE', 'MORPH_CODE',\
                   'CANCER_SITE', 'GRADE', 'TUMOUR_SIZE', 'STAGE']

# --- 2) Year-range completeness (%) and N computed the same way as overall ---
completeness_pct = (CaSumFiltered_7.groupby('year_range')[all_cancer_cols]
                    .apply(lambda df: df.notnull().mean() * 100).T )

# counts (N) per year_range
completeness_n = (CaSumFiltered_7.groupby('year_range')[all_cancer_cols]
                .apply(lambda df: df.notnull().sum()).T)

# Rename columns so they are explicit when merged
completeness_pct = completeness_pct.rename(columns=lambda c: f"{c} Completeness (%)")
completeness_n = completeness_n.rename(columns=lambda c: f"{c} N")
completeness_n = completeness_n.reset_index().rename(columns={'index': 'Column3'})
completeness_pct = completeness_pct.reset_index().rename(columns={'index': 'Column2'})

overall_completeness = pd.DataFrame({
                        'Completeness (%)': CaSumFiltered_7[all_cancer_cols].notnull().mean() * 100,
                        'N': CaSumFiltered_7[all_cancer_cols].notnull().sum()})

overall_completeness = overall_completeness.reset_index().rename(columns={'index': 'Column'})

# --- 3) Merge overall + per-year-range into single dataframe ---
overall_complete = pd.concat([overall_completeness, completeness_pct, completeness_n], axis=1)

overall_complete = overall_complete.round(decimals=2)
overall_complete = overall_complete.drop(['Column2', 'Column3'], axis=1)

overall_complete = overall_complete[['Column', 'Completeness (%)', 'N', 
                                    '<2004 Completeness (%)', '<2004 N',
                                    '2005-2009 Completeness (%)', '2005-2009 N', 
                                    '2010-2014 Completeness (%)', '2010-2014 N',
                                    '2015-2019 Completeness (%)', '2015-2019 N',
                                    '2020-2025 Completeness (%)', '2020-2025 N']]

#%%
# Completeness of data - breast variables
br_cols = ['ER_STATUS', 'PR_STATUS', 'HER2_STATUS', 'HER2_FISH', 'Ki67', 'SCREEN_DETECTED',\
               'SCREENINGSTATUSCOSD_CODE','LATERALITY', 'T_STAGE', 'N_STAGE', 'M_STAGE', \
               'TUMOUR_SIZE', 'NODES_TOTAL', 'NODES_POSITIVE']
br_subset = CaSumFiltered_7[CaSumFiltered_7['CANCER_SITE']=='breast'].copy()

# --- 2) Year-range completeness (%) and N computed the same way as overall ---
completeness_pct = (CaSumFiltered_7.groupby('year_range')[br_cols]
                    .apply(lambda df: df.notnull().mean() * 100).T )

# counts (N) per year_range
completeness_n = (CaSumFiltered_7.groupby('year_range')[br_cols]
                .apply(lambda df: df.notnull().sum()).T)

# Rename columns so they are explicit when merged
completeness_pct = completeness_pct.rename(columns=lambda c: f"{c} Completeness (%)")
completeness_n = completeness_n.rename(columns=lambda c: f"{c} N")
completeness_n = completeness_n.reset_index().rename(columns={'index': 'Column3'})
completeness_pct = completeness_pct.reset_index().rename(columns={'index': 'Column2'})

br_completeness = pd.DataFrame({
                    'Completeness (%)': br_subset[br_cols].notnull().mean() * 100,
                    'N': br_subset[br_cols].notnull().sum()})

br_completeness = br_completeness.reset_index().rename(columns={'index': 'Column'})

# --- 3) Merge overall + per-year-range into single dataframe ---
br_complete = pd.concat([br_completeness, completeness_pct, completeness_n], axis=1)

br_complete = br_complete.round(decimals=2)
br_complete = br_complete.drop(['Column2', 'Column3'], axis=1)

br_complete = br_complete[['Column', 'Completeness (%)', 'N', 
                                    '<2004 Completeness (%)', '<2004 N',
                                    '2005-2009 Completeness (%)', '2005-2009 N', 
                                    '2010-2014 Completeness (%)', '2010-2014 N',
                                    '2015-2019 Completeness (%)', '2015-2019 N',
                                    '2020-2025 Completeness (%)', '2020-2025 N']]

#%% Grouped by SITE
CaSumFiltered_7['GROUPED_SITE'] = CaSumFiltered_7['ICD_CODE'].apply(sm.group_sites)
site_groups = CaSumFiltered_7.groupby('GROUPED_SITE')[['STUDY_ID']].size().reset_index(name='Count')

# Grouped by SOURCE for all cancers
source_groups = CaSumFiltered_7.groupby('S_STUDY_ID')[['STUDY_ID']].size().reset_index(name='Count')

# Grouped by SOURCE for breast cancer incidents
CaSumFiltered_8 = CaSumFiltered_7[CaSumFiltered_7['CANCER_SITE']=='breast'].copy()
br_source_groups = CaSumFiltered_8.groupby('S_STUDY_ID')[['STUDY_ID']].size().reset_index(name='Count')

#%% Year of diagnosis range with data source % contributed
br_source_percent = CaSumFiltered_7[CaSumFiltered_7['CANCER_SITE']=='breast'].copy()

source_percent = (br_source_percent.groupby(['year_range', 'S_STUDY_ID'])['STUDY_ID']
          .sum().groupby(level=0)
          .apply(lambda x: 100 * x / x.sum())
          .reset_index(name='%_contribution'))

source_percent = source_percent[source_percent['%_contribution'].notna()]
source_percent['year_range'] = source_percent['year_range'].astype(str)

# PIVOT SO EACH YEAR RANGE IS A COLUMN
pivoted = source_percent.pivot(index='S_STUDY_ID', columns='year_range', values='%_contribution').reset_index()
pivoted = pivoted.rename(columns={'S_STUDY_ID':'Source'})
pivoted = pivoted.round(decimals=2)

#%% save the reports to an excel file
reports = [('Overall Completeness', overall_complete),
           ('Breast variable Completeness', br_complete),
           ('Groups by SITE', site_groups),
           ('Groups by Source', source_groups),
           ('Groups by Source (Breast cases)', br_source_groups),
           ('YearOfDiag by Source', pivoted)]

with pd.ExcelWriter(os.path.join(cf.casum_report_path, summary_path)) as writer:
    for rpt_name, data in reports:
        sheet = rpt_name
        data.to_excel(writer, sheet_name=sheet, index=False)