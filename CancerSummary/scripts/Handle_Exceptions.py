# -*- coding: utf-8 -*-
"""
Created on Thu Oct 23 15:28:18 2025

@author: shegde
purpose: Set of functions tio handle exceptional cases
"""

import os
os.chdir('N:\CancerEpidem\BrBreakthrough\SHegde\Schema_and_Derivation_utils\CancerSummary\scripts')
import sys
sys.path.append(os.path.abspath('N:\CancerEpidem\BrBreakthrough\SHegde\Schema_and_Derivation_utils'))

import pandas as pd

def expand_registry_laterality(df):
    """
    Handles special Registry ↔ HistoPath_BrCa laterality logic:
      - If Registry laterality is '9': replace with HistoPath laterality (if single).
      - If Registry laterality is 'B': split into multiple rows, one per HistoPath laterality (L/R).
      - Remove the matched HistoPath_BrCa rows after processing.
    """

    df = df.copy()
    new_rows = []

    # Separate datasets for convenience
    registry_df = df[df["S_STUDY_ID"] == "CancerRegistry_0125"]
    histo_df = df[df["S_STUDY_ID"] == "HistoPath_BrCa"]

    # Index HistoPath laterality by match keys
    histo_group = (histo_df.groupby(["STUDY_ID", "DIAGNOSIS_DATE", "MORPH_CODE"])["LATERALITY"]
                    .apply(list).to_dict())

    to_drop = set()

    for idx, row in registry_df.iterrows():
        key = (row["STUDY_ID"], row["DIAGNOSIS_DATE"], row["MORPH_CODE"])
        histo_lats = histo_group.get(key, [])

        if row["LATERALITY"] in ["9", "B"] and histo_lats:
            if row["LATERALITY"] == "9":
                # Replace laterality with first available from HistoPath
                new_row = row.copy()
                new_row["LATERALITY"] = histo_lats[0]
                new_row["S_LATERALITY"] = "HistoPath_BrCa.LATERALITY"
                new_rows.append(new_row)

            elif row["LATERALITY"] == "B":
                # Split row for all distinct HistoPath laterality values
                for lat in sorted(set(histo_lats)):
                    new_row = row.copy()
                    new_row["LATERALITY"] = lat
                    new_row["S_LATERALITY"] = "HistoPath_BrCa.LATERALITY"
                    new_rows.append(new_row)
            
            to_drop.update(histo_df[(histo_df["STUDY_ID"] == row["STUDY_ID"])
                            & (histo_df["DIAGNOSIS_DATE"] == row["DIAGNOSIS_DATE"])
                            & (histo_df["MORPH_CODE"] == row["MORPH_CODE"])].index)

        elif histo_lats and all(lat != row["LATERALITY"] for lat in histo_lats):
            new_row = row.copy()
            new_rows.append(new_row)
            
            to_drop.update(histo_df[(histo_df["STUDY_ID"] == row["STUDY_ID"])
                            & (histo_df["DIAGNOSIS_DATE"] == row["DIAGNOSIS_DATE"])
                            & (histo_df["MORPH_CODE"] == row["MORPH_CODE"])].index)
            
        else:
            new_rows.append(row)

    # Add all other non-registry
    other_sources = df[~df["S_STUDY_ID"].isin(["CancerRegistry_0125"])]
    
    # Drop matched HistoPath_BrCa rows
    other_sources = other_sources[~other_sources.index.isin(to_drop)].reset_index(drop=True)
    
    new_df = pd.concat([pd.DataFrame(new_rows), other_sources], ignore_index=True)
    
    return new_df


def resolve_morph_code_conflicts(df: pd.DataFrame, registry_sources) -> pd.DataFrame:
    """
    Resolves MORPH_CODE conflicts only when Registry and other sources differ.
    Keeps Registry rows; drops conflicting non-Registry rows.
    If no conflict, all rows are kept.
    """

    df = df.copy()
    df["_orig_index"] = df.index
    new_rows = []

    # Choose grouping rule
    if {"CancerRegistry_0125", "HistoPath_BrCa"}.issubset(registry_sources):
        grouped = df.groupby(["STUDY_ID", "DIAGNOSIS_DATE", "LATERALITY"])
    else:
        grouped = df.groupby(["STUDY_ID", "DIAGNOSIS_DATE"])

    for _, group in grouped:

        # Single-row group -> keep
        if len(group) == 1:
            new_rows.append(group.iloc[0].to_dict())
            continue

        # If cancer sites differ → keep all
        if group["CANCER_SITE"].nunique(dropna=True) > 1:
            new_rows.extend(group.to_dict("records"))
            continue

        # Check if MORPH_CODE conflict exists
        morphs = group["MORPH_CODE"].dropna().unique()
        if len(morphs) <= 1:
            # No conflict, keep all rows
            new_rows.extend(group.to_dict("records"))
            continue

        # Conflict exists → keep only Registry rows
        registry_rows = group[group["S_STUDY_ID"].isin(registry_sources)]
        if not registry_rows.empty:
            new_rows.extend(registry_rows.to_dict("records"))
        else:
            # No Registry row → keep all
            new_rows.extend(group.to_dict("records"))

    # Convert to DataFrame
    result = pd.DataFrame(new_rows)

    # Drop duplicates based on original index
    if "_orig_index" in result.columns:
        result = result.drop_duplicates(subset="_orig_index")

    result = result.drop(columns=["_orig_index"], errors="ignore")
    return result


def resolve_icd_code_conflicts(df: pd.DataFrame, registry_sources) -> pd.DataFrame:
    """
    Resolves ICD_CODE conflicts only when Registry and other sources differ.
    Keeps Registry rows; drops conflicting non-Registry rows.
    If no conflict, all rows are kept.
    """
    df = df.copy()
    df["_orig_index"] = df.index
    new_rows = []

    # Choose grouping rule
    if {"CancerRegistry_0125", "HistoPath_BrCa"}.issubset(registry_sources):
        grouped = df.groupby(["STUDY_ID", "DIAGNOSIS_DATE", "LATERALITY"])
    else:
        grouped = df.groupby(["STUDY_ID", "DIAGNOSIS_DATE"])

    for _, group in grouped:

        # Single-row group -> keep
        if len(group) == 1:
            new_rows.append(group.iloc[0].to_dict())
            continue

        # If cancer sites differ → keep all
        if group["CANCER_SITE"].nunique(dropna=True) > 1:
            new_rows.extend(group.to_dict("records"))
            continue

        # Check if ICD_CODE conflict exists
        icd_codes = group["ICD_CODE"].dropna().unique()
        if len(icd_codes) <= 1:
            # No conflict, keep all rows
            new_rows.extend(group.to_dict("records"))
            continue

        # Conflict exists → keep only Registry rows
        registry_rows = group[group["S_STUDY_ID"].isin(registry_sources)]
        if not registry_rows.empty:
            new_rows.extend(registry_rows.to_dict("records"))
        else:
            # No Registry row → keep all
            new_rows.extend(group.to_dict("records"))

    # Convert to DataFrame
    result = pd.DataFrame(new_rows)

    # Drop duplicates based on original index
    if "_orig_index" in result.columns:
        result = result.drop_duplicates(subset="_orig_index")

    result = result.drop(columns=["_orig_index"], errors="ignore")
    return result


