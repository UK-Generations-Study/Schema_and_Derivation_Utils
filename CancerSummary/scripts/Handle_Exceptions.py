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
import re

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
    registry_df = df[df["S_STUDY_ID"] == "CancerRegistry.STUDY_ID"]
    histo_df = df[df["S_STUDY_ID"].str.contains("PHE|HistoPath_BrCa")]

    # Index HistoPath laterality by match keys
    histo_group = (histo_df.groupby(["STUDY_ID", "DIAGNOSIS_DATE", "ICD_CODE"])["LATERALITY"]
                    .apply(list).to_dict())

    to_drop = set()

    for idx, row in registry_df.iterrows():
        key = (row["STUDY_ID"], row["DIAGNOSIS_DATE"], row["ICD_CODE"])
        histo_lats = histo_group.get(key, [])

        if row["LATERALITY"] in ["9", "B", "M"] and histo_lats:
            if row["LATERALITY"] == "9":
                # Replace laterality with first available from HistoPath
                new_row = row.copy()
                new_row["LATERALITY"] = histo_lats[0]
                new_row["S_LATERALITY"] = new_row["S_STUDY_ID"]
                new_rows.append(new_row)

            elif row["LATERALITY"] == "B":
                # Split row for all distinct HistoPath laterality values
                for lat in sorted(set(histo_lats)):
                    new_row = row.copy()
                    new_row["LATERALITY"] = lat
                    new_row["S_LATERALITY"] = new_row["S_STUDY_ID"]
                    new_rows.append(new_row)
                
            elif row["LATERALITY"] == "M":
                # Split row for all distinct HistoPath laterality values
                for lat in sorted(set(histo_lats)):
                    new_row = row.copy()
                    new_row["LATERALITY"] = lat
                    new_row["S_LATERALITY"] = new_row["S_STUDY_ID"]
                    new_rows.append(new_row)
            
            to_drop.update(histo_df[(histo_df["STUDY_ID"] == row["STUDY_ID"])
                            & (histo_df["DIAGNOSIS_DATE"] == row["DIAGNOSIS_DATE"])
                            & (histo_df["ICD_CODE"] == row["ICD_CODE"])].index)

        elif histo_lats and all(lat != row["LATERALITY"] for lat in histo_lats):
            new_row = row.copy()
            new_rows.append(new_row)
            
            to_drop.update(histo_df[(histo_df["STUDY_ID"] == row["STUDY_ID"])
                            & (histo_df["DIAGNOSIS_DATE"] == row["DIAGNOSIS_DATE"])
                            & (histo_df["ICD_CODE"] == row["ICD_CODE"])].index)
            
        else:
            new_rows.append(row)

    # Add all other non-registry
    other_sources = df[~df["S_STUDY_ID"].isin(["CancerRegistry.STUDY_ID"])]
    
    # Drop matched HistoPath_BrCa rows
    other_sources = other_sources[~other_sources.index.isin(to_drop)].reset_index(drop=True)
    
    new_df = pd.concat([pd.DataFrame(new_rows), other_sources], ignore_index=True)
    
    return new_df


def _extract_suffix_number(source: str) -> int:
    """
    Extract last 2-digit number from source name.
    If not present, return -1 so it loses.
    """
    if not isinstance(source, str):
        return -1
    match = re.search(r"(\d{2})$", source)
    return int(match.group(1)) if match else -1


def resolve_morph_code_conflicts(df: pd.DataFrame, registry_sources):
    """
    Resolves MORPH_CODE conflicts only when Registry and other sources differ.
    Returns:
        - resolved_df
        - dropped_df (for debugging)
    """
    df = df.copy()
    df["_orig_index"] = df.index

    dropped_idx = set()

    # Grouping rule
    if {"CancerRegistry_0125", "HistoPath_BrCa"}.issubset(registry_sources):
        grouped = df.groupby(["STUDY_ID", "DIAGNOSIS_DATE", "LATERALITY"])
    else:
        grouped = df.groupby(["STUDY_ID", "DIAGNOSIS_DATE"])

    for _, group in grouped:
        
        # check for required sources
        if not set(registry_sources).issubset(set(group['S_STUDY_ID'])):
            continue

        # Single row → nothing to resolve
        if len(group) == 1:
            continue

        # Different cancer sites → keep all
        if group["CANCER_SITE"].nunique(dropna=True) > 1:
            continue

        # No ICD conflict → keep all
        icd_codes = group["MORPH_CODE"].dropna().unique()
        if len(icd_codes) <= 1:
            continue

        # Conflict exists
        # FlaggingCancers MUST be dropped
        flagging_idx = group.index[group["S_STUDY_ID"] == "FlaggingCancers"]
        dropped_idx.update(flagging_idx)
        
        # Drop FlaggingCancers only if other sources exist
        non_flagging = group[group["S_STUDY_ID"] != "FlaggingCancers"]
        candidates = non_flagging if not non_flagging.empty else group

        # Pick winner by max numeric suffix
        suffix = candidates['S_STUDY_ID'].apply(_extract_suffix_number)
        max_suffix = suffix.max()

        if (suffix == max_suffix).sum() > 1:
            continue
         
        winner_idx = suffix.idxmax()

        # Everything else is dropped
        dropped_idx.update(
            candidates.index.difference([winner_idx])
        )

        # If FlaggingCancers was excluded, drop it too
        dropped_idx.update(
            group.index.difference(candidates.index)
        )

    resolved_df = df.loc[~df["_orig_index"].isin(dropped_idx)].copy()
    dropped_df = df.loc[df["_orig_index"].isin(dropped_idx)].copy()

    resolved_df = resolved_df.drop(columns="_orig_index", errors="ignore")
    dropped_df = dropped_df.drop(columns="_orig_index", errors="ignore")

    return resolved_df, dropped_df


def resolve_icd_code_conflicts(df: pd.DataFrame, registry_sources):
    """
    Resolves ICD_CODE conflicts by keeping exactly ONE row per tumour
    when conflicts exist.

    Rules:
    - If no ICD_CODE conflict → keep all rows
    - If conflict exists:
        - Drop HistoPath_BrCa if other sources exist
        - From remaining rows, keep the one with the
          highest 2-digit numeric suffix in S_STUDY_ID

    Returns:
        resolved_df, dropped_df
    """

    df = df.copy()
    df["_orig_index"] = df.index

    dropped_idx = set()

    # Grouping rule
    if {"CancerRegistry_0125", "HistoPath_BrCa"}.issubset(registry_sources):
        grouped = df.groupby(["STUDY_ID", "DIAGNOSIS_DATE", "LATERALITY"])
    else:
        grouped = df.groupby(["STUDY_ID", "DIAGNOSIS_DATE"])

    for _, group in grouped:
        
        # check for required sources
        if not set(registry_sources).issubset(set(group['S_STUDY_ID'])):
            continue

        # Single row → nothing to resolve
        if len(group) == 1:
            continue

        # Different cancer sites → keep all
        if group["CANCER_SITE"].nunique(dropna=True) > 1:
            continue

        # No ICD conflict → keep all
        icd_codes = group["ICD_CODE"].dropna().unique()
        if len(icd_codes) <= 1:
            continue

        # Conflict exists
        # HistoPath_BrCa MUST be dropped
        histopath_idx = group.index[group["S_STUDY_ID"] == "HistoPath_BrCa"]
        dropped_idx.update(histopath_idx)
        
        # Drop HistoPath_BrCa only if other sources exist
        non_histopath = group[group["S_STUDY_ID"] != "HistoPath_BrCa"]
        candidates = non_histopath if not non_histopath.empty else group

        # Pick winner by max numeric suffix
        suffix = candidates['S_STUDY_ID'].apply(_extract_suffix_number)
        max_suffix = suffix.max()
        if (suffix==max_suffix).sum() > 1:
            continue
         
        winner_idx = suffix.idxmax()

        # Everything else is dropped
        dropped_idx.update(
            candidates.index.difference([winner_idx])
        )

        # If HistoPath_BrCa was excluded, drop it too
        dropped_idx.update(
            group.index.difference(candidates.index)
        )

    resolved_df = df.loc[~df["_orig_index"].isin(dropped_idx)].copy()
    dropped_df = df.loc[df["_orig_index"].isin(dropped_idx)].copy()

    resolved_df = resolved_df.drop(columns="_orig_index", errors="ignore")
    dropped_df = dropped_df.drop(columns="_orig_index", errors="ignore")

    return resolved_df, dropped_df