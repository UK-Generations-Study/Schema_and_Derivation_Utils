import os
os.chdir('N:\CancerEpidem\BrBreakthrough\SHegde\Schema_and_Derivation_utils\CancerSummary\scripts')
import sys
sys.path.append(os.path.abspath('N:\CancerEpidem\BrBreakthrough\SHegde\Schema_and_Derivation_utils'))

import pandas as pd
import numpy as np
import config as cf

source_path = "Tumour_Source_Mapping_20251110.xlsx"
output = "Crosstabs.xlsx"

data = pd.read_excel(os.path.join(cf.casum_report_path, source_path))

#%%
df = data[data['S_STUDY_ID']!='HistoPath_OvCa']

# Only breast cancer incidents
df1 = df[df['ICD_CODE'].str.startswith('C50') | df['CancerICD'].str.startswith('C50') | \
        df['ICD_CODE'].str.startswith('D05') | df['CancerICD'].str.startswith('D05') |
        df['CancerICD'].str.startswith('17')]

df2 = df[(df['S_STUDY_ID']=='HistoPath_BrCa')]

df3 = pd.concat([df1, df2])

#%%
def crosstabs(df, col1, col2):
    
    # Aggregate per cluster: take first non-null for each column, but keep NaN clusters
    agg_df = df.groupby('Cluster_ID')[[col1, col2]].agg(
        lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else np.nan
    ).reset_index(drop=True)
    
    if agg_df.empty:
        return pd.DataFrame()
    
    # Crosstab — keep NaNs visible
    crosstab = pd.crosstab(
        index=agg_df[col1].fillna("NULL"),
        columns=agg_df[col2].fillna("NULL"),
        dropna=False,
        rownames=["Others ↓ Path Report →"])
    
    return crosstab

column_pairs = [('ER_STATUS', 'ER_Status'),
                ('PR_STATUS', 'PR_Status'),
                ('HER2_STATUS', 'HER2_Status'),
                ('SCREEN_DETECTED', 'ScreenDetected')]

with pd.ExcelWriter(os.path.join(cf.casum_report_path, output)) as writer:
    for col1, col2 in column_pairs:
        result = crosstabs(df3, col1, col2)
        sheet = f"{col1}_vs_{col2}"[:31]
        result.to_excel(writer, sheet_name=sheet)

#%% REPORTING
'''
# removed PHE_0125
Summary = CancerSummary[~CancerSummary['S_STUDY_ID'].str.contains('PHE_0125')]

# Flagging Deaths grouped by SITE
df_deaths = Summary[Summary['STUDY_ID'].isin(flagging_deaths_link['STUDY_ID'])]
groups = df_deaths.groupby(['CANCER_SITE'])[['STUDY_ID']].count()

# bilateral cases in Registry vs Path report
bilateral = CaSumFiltered_3[CaSumFiltered_3['LATERALITY']=='B']\
                            [['STUDY_ID','S_STUDY_ID' , 'CANCER_SITE', 'MORPH_CODE', 'LATERALITY', 'S_LATERALITY']]

bilateral_br = brca_link[brca_link['STUDY_ID'].isin(bilateral['STUDY_ID'])]
bilateral_groups = bilateral.groupby('CANCER_SITE')[['STUDY_ID']].count()

# PHE_0125 is the latest registry dataset
phe = CancerSummary[CancerSummary['S_STUDY_ID'].str.contains('PHE_0125') | CancerSummary['S_STUDY_ID'].str.contains('CancerRegistry')]

phe_source = phe.groupby('STUDY_ID')[['S_STUDY_ID', 'MORPH_CODE', 'LATERALITY']].agg(list)
phe_short = phe[phe['STUDY_ID'].isin([101897, 341102])][['STUDY_ID', 'S_STUDY_ID', 'DIAGNOSIS_DATE', 'LATERALITY', 'MORPH_CODE']]
required = {'PHE_0125', 'CancerRegistry.STUDY_ID'}
path_and_new_both = phe_source[phe_source['S_STUDY_ID'].apply(lambda x: required.issubset(set(x)))]

# different MORPH_CODES
required = {'CancerRegistry.STUDY_ID', 'FlaggingCancers.STUDY_ID'}

fl_and_reg_ICDM = CaSumFiltered_4[CaSumFiltered_4['S_STUDY_ID'].isin(['CancerRegistry.STUDY_ID', 'FlaggingCancers.STUDY_ID'])]\
                        .groupby(['STUDY_ID', 'DIAGNOSIS_DATE'])[['STUDY_ID', 'S_STUDY_ID', 'DIAGNOSIS_DATE','MORPH_CODE', \
                                'CANCER_SITE', 'ICD_CODE']].agg(list)

fl_and_reg_ICDM_both = fl_and_reg_ICDM[fl_and_reg_ICDM['S_STUDY_ID'].apply(lambda x: required.issubset(set(x)))]

required = {'HistoPath_BrCa.STUDY_ID', 'FlaggingCancers.STUDY_ID'}

path_and_reg_ICDM = Summary[Summary['S_STUDY_ID'].isin(['HistoPath_BrCa.STUDY_ID', 'FlaggingCancers.STUDY_ID'])]\
                        .groupby(['STUDY_ID', 'DIAGNOSIS_DATE'])[['STUDY_ID', 'S_STUDY_ID', 'DIAGNOSIS_DATE','MORPH_CODE']].agg(list)

path_and_reg_ICDM_both = path_and_reg_ICDM[path_and_reg_ICDM['S_STUDY_ID'].apply(lambda x: required.issubset(set(x)))]

required = {'HistoPath_BrCa.STUDY_ID', 'CancerRegistry.STUDY_ID'}

laterality_check = CaSumFiltered_3[CaSumFiltered_3['S_STUDY_ID'].isin(['HistoPath_BrCa.STUDY_ID', 'CancerRegistry.STUDY_ID'])]\
                        .groupby(['STUDY_ID', 'DIAGNOSIS_DATE', 'LATERALITY'])[['S_STUDY_ID', 'CANCER_SITE', 'MORPH_CODE']].agg(list)

checked = laterality_check[laterality_check['S_STUDY_ID'].apply(lambda x: required.issubset(set(x)))]    

# different laterality
required = {'HistoPath_BrCa.STUDY_ID', 'CancerRegistry.STUDY_ID'}

laterality_check = CaSumFiltered_3[CaSumFiltered_3['S_STUDY_ID'].isin(['HistoPath_BrCa.STUDY_ID', 'CancerRegistry.STUDY_ID'])]\
                        .groupby(['STUDY_ID', 'DIAGNOSIS_DATE', 'MORPH_CODE'])[['S_STUDY_ID', 'LATERALITY']].agg(list)

checked = laterality_check[laterality_check['S_STUDY_ID'].apply(lambda x: required.issubset(set(x)))]

benign = CaSumFiltered_7[CaSumFiltered_7['GROUPED_SITE']=='benign'][['S_STUDY_ID', 'DIAGNOSIS_DATE', 'ICD_CODE', 'MORPH_CODE',\
                   'GROUPED_SITE']]

unkn_uncert = CaSumFiltered_7[CaSumFiltered_7['GROUPED_SITE']=='unknown/uncertain'][['S_STUDY_ID', 'DIAGNOSIS_DATE', 'ICD_CODE', 'MORPH_CODE',\
                   'GROUPED_SITE']]

benign_source = benign.groupby('S_STUDY_ID')[['S_STUDY_ID']].size().reset_index(name='Count')

unkn_uncert_source = unkn_uncert.groupby('S_STUDY_ID')[['S_STUDY_ID']].size().reset_index(name='Count')


missing_mc = CaSumFiltered_7[CaSumFiltered_7['MORPH_CODE'].isna()][['S_STUDY_ID', 'DIAGNOSIS_DATE', 'ICD_CODE', 'MORPH_CODE',\
                   'GROUPED_SITE']]

missing_mc_source = missing_mc.groupby('S_STUDY_ID')[['S_STUDY_ID']].size().reset_index(name='Count')
'''