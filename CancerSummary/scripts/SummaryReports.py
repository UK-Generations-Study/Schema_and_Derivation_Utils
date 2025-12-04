# -*- coding: utf-8 -*-
"""
Created on Tue Jul 1 10:20:27 2025

@author: shegde
purpose: Generate summary reports for Cancer Summary dataset
"""

import os
os.chdir('N:\CancerEpidem\BrBreakthrough\SHegde\Schema_and_Derivation_utils\CancerSummary\scripts')
import sys
sys.path.append(os.path.abspath('N:\CancerEpidem\BrBreakthrough\SHegde\Schema_and_Derivation_utils'))
import config as cf

import pandas as pd

def generate_summary_reports(CaSumFiltered_7, summary_path):
    '''
    Generates all the summary reports for the cancer summary dataset
    args:
        CaSumFiltered_7 (dataframe): Input dataframe with latest cancer summary data
        summary_path (string): File name to save the reports    
    '''    

    # Completeness of data - overall
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
    completeness_pct = completeness_pct.rename(columns=lambda c: f"{c} (%)")
    completeness_n = completeness_n.rename(columns=lambda c: f"{c} N")
    completeness_n = completeness_n.reset_index().rename(columns={'index': 'Column3'})
    completeness_pct = completeness_pct.reset_index().rename(columns={'index': 'Column2'})
    
    overall_completeness = pd.DataFrame({
                            '(%)': CaSumFiltered_7[all_cancer_cols].notnull().mean() * 100,
                            'N': CaSumFiltered_7[all_cancer_cols].notnull().sum()})
    
    overall_completeness = overall_completeness.reset_index().rename(columns={'index': 'Column'})
    
    # --- 3) Merge overall + per-year-range into single dataframe ---
    overall_complete = pd.concat([overall_completeness, completeness_pct, completeness_n], axis=1)
    
    overall_complete = overall_complete.round(decimals=2)
    overall_complete = overall_complete.drop(['Column2', 'Column3'], axis=1)
    
    overall_complete = overall_complete[['Column', '(%)', 'N', 
                                        '<2004 (%)', '<2004 N',
                                        '2005-2009 (%)', '2005-2009 N', 
                                        '2010-2014 (%)', '2010-2014 N',
                                        '2015-2019 (%)', '2015-2019 N',
                                        '2020-2025 (%)', '2020-2025 N']]
    
    #
    # Completeness of data - breast variables
    br_cols = ['ER_STATUS', 'PR_STATUS', 'HER2_STATUS', 'HER2_FISH', 'Ki67', 'SCREEN_DETECTED',\
                   'SCREENINGSTATUSCOSD_CODE','LATERALITY', 'T_STAGE', 'N_STAGE', 'M_STAGE', \
                   'TUMOUR_SIZE', 'NODES_TOTAL', 'NODES_POSITIVE']
    br_subset = CaSumFiltered_7[CaSumFiltered_7['CANCER_SITE']=='breast'].copy()
    
    # --- 2) Year-range completeness (%) and N computed the same way as overall ---
    completeness_pct = (br_subset.groupby('year_range')[br_cols]
                        .apply(lambda df: df.notnull().mean() * 100).T )
    
    # counts (N) per year_range
    completeness_n = (br_subset.groupby('year_range')[br_cols]
                    .apply(lambda df: df.notnull().sum()).T)
    
    # Rename columns so they are explicit when merged
    completeness_pct = completeness_pct.rename(columns=lambda c: f"{c} (%)")
    completeness_n = completeness_n.rename(columns=lambda c: f"{c} N")
    completeness_n = completeness_n.reset_index().rename(columns={'index': 'Column3'})
    completeness_pct = completeness_pct.reset_index().rename(columns={'index': 'Column2'})
    
    br_completeness = pd.DataFrame({
                        '(%)': br_subset[br_cols].notnull().mean() * 100,
                        'N': br_subset[br_cols].notnull().sum()})
    
    br_completeness = br_completeness.reset_index().rename(columns={'index': 'Column'})
    
    # --- 3) Merge overall + per-year-range into single dataframe ---
    br_complete = pd.concat([br_completeness, completeness_pct, completeness_n], axis=1)
    
    br_complete = br_complete.round(decimals=2)
    br_complete = br_complete.drop(['Column2', 'Column3'], axis=1)
    
    br_complete = br_complete[['Column', '(%)', 'N', 
                                        '<2004 (%)', '<2004 N',
                                        '2005-2009 (%)', '2005-2009 N', 
                                        '2010-2014 (%)', '2010-2014 N',
                                        '2015-2019 (%)', '2015-2019 N',
                                        '2020-2025 (%)', '2020-2025 N']]
    
    # Grouped by SITE
    site_groups = CaSumFiltered_7.groupby('GROUPED_SITE')[['STUDY_ID']].size().reset_index(name='Count')
    site_groups.rename(columns={'STUDY_ID':'Source'}, inplace=True)
    
    # Grouped by SOURCE for all cancers
    source_groups = CaSumFiltered_7.groupby('S_STUDY_ID')[['STUDY_ID']].size().reset_index(name='Count')
    source_groups.rename(columns={'STUDY_ID':'Source'}, inplace=True)
    
    # Grouped by SOURCE for breast cancer incidents
    CaSumFiltered_8 = CaSumFiltered_7[CaSumFiltered_7['CANCER_SITE']=='breast'].copy()
    br_source_groups = CaSumFiltered_8.groupby('S_STUDY_ID')[['STUDY_ID']].size().reset_index(name='Count')
    
    # GRADE, STAGE, and TUMOUR SIZE by SITE
    three_var_site_groups = CaSumFiltered_7.groupby('GROUPED_SITE')[['GRADE', 'STAGE', 'TUMOUR_SIZE']].agg(lambda x: x.notna().sum())\
                            .reset_index().rename(columns={'index': 'GROUPED_SITE'})
    
    # Filter to breast cancer
    br_source_percent = CaSumFiltered_7[CaSumFiltered_7['CANCER_SITE'] == 'breast'].copy()

    tmp = (br_source_percent
        .groupby(['year_range', 'S_STUDY_ID'])['STUDY_ID']
        .sum()
        .groupby(level=0)
        .transform(lambda x: 100 * x / x.sum()))

    # Fix: ensure MultiIndex has the correct names BEFORE reset_index
    tmp.index = tmp.index.set_names(['year_range', 'S_STUDY_ID'])

    # Convert Series -> DataFrame safely
    source_percent = tmp.reset_index(name='%_contribution')
    source_percent['year_range'] = source_percent['year_range'].astype(str)
    source_percent = source_percent[source_percent['%_contribution'].notna()]

    pivoted = (source_percent.pivot(index='S_STUDY_ID', columns='year_range', values='%_contribution').reset_index())

    pivoted = pivoted.rename(columns={'S_STUDY_ID': 'Source'})
    pivoted = pivoted.round(decimals=2)
    
    # save the reports to an excel file
    reports = [('Overall Completeness', overall_complete),
               ('Breast variable Completeness', br_complete),
               ('Non-Null count by SITE (only 3)', three_var_site_groups),
               ('Groups by SITE', site_groups),
               ('Groups by Source', source_groups),
               ('Groups by Source (Breast)', br_source_groups),
               ('YearOfDiag by Source (Breast)', pivoted)]
    
    with pd.ExcelWriter(os.path.join(cf.casum_report_path, summary_path)) as writer:
        for rpt_name, data in reports:
            sheet = rpt_name
            data.to_excel(writer, sheet_name=sheet, index=False)