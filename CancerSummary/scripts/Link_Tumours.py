# -*- coding: utf-8 -*-
"""
Created on Thu Sep 11 14:50:18 2025

@author: shegde
purpose: Set of functions used to link tumours
"""

import os
os.chdir('N:\CancerEpidem\BrBreakthrough\SHegde\Schema_and_Derivation_utils\CancerSummary\scripts')
import sys
sys.path.append(os.path.abspath('N:\CancerEpidem\BrBreakthrough\SHegde\Schema_and_Derivation_utils'))

import pandas as pd
import time
import numpy as np
from typing import List
import config as cf


def tumour_source_mapping(clusters, final_cancer_summary, filename="Tumour_Source_Mapping"):
    """
    Saves a debug Excel file showing tumours from all sources with their assigned clusters
    and includes the final selected values for comparison
    
    Args:
        clusters (list): List of clusters from build_clusters_optimized()
        final_cancer_summary (pd.DataFrame): The final CancerSummary dataframe
        filename (str): Base filename for the output
    """
    debug_records = []

    for cluster_id, cluster in enumerate(clusters):
        # Skip empty clusters
        if not cluster:
            continue

        # Safely get final row for this cluster if available
        final_row = (
            final_cancer_summary.iloc[cluster_id].to_dict()
            if cluster_id < len(final_cancer_summary)
            else {}
        )

        for entry in cluster:
            record = {
                "Cluster_ID": cluster_id,
                "S_STUDY_ID": entry.get("source"),
                "STUDY_ID": entry.get("STUDY_ID"),
                "DIAGNOSIS_DATE": entry.get("DIAGNOSIS_DATE"),
                "MORPH_CODE": entry.get("MORPH_CODE"),
                "LATERALITY": entry.get("LATERALITY"),
                "Is_Final_Selection": False,
            }

            # Add all columns from original row (non-conflicting)
            row_data = entry.get("row", {})
            if hasattr(row_data, "items"):
                for col_name, value in row_data.items():
                    if col_name not in record:
                        record[col_name] = value

            # Identify if this record's source contributed to any final variable
            for col_name, final_value in final_row.items():
                if col_name.startswith("S_"):
                    source_field = final_value
                    # Example: S_STAGE = "FlaggingCancers.STAGE" or "Legacy"
                    if source_field:
                        if record["S_STUDY_ID"] in str(source_field):
                            record["Is_Final_Selection"] = True
                            break

            debug_records.append(record)

    debug_df = pd.DataFrame(debug_records)

    # Optional: sort for readability
    debug_df = debug_df.sort_values(["STUDY_ID", "Cluster_ID", "S_STUDY_ID", "DIAGNOSIS_DATE"]).reset_index(drop=True)

    # Save to Excel
    debug_df.to_excel(str(os.path.join(cf.casum_report_path, filename)) + '_' + time.strftime("%Y%m%d") + '.xlsx', index=False)


# get_linking_rules
def get_linking_rules(source1: str, source2: str) -> bool:
    """
    Returns True if the two sources are allowed to be linked

    Args:
        source1 (string): Source name for input row 1
        source2 (string): Source name for input row 2
    Returns: Boolean object
    """
    sources = tuple(sorted([source1, source2]))

    linkable_pairs = {
        ('CancerRegistry', 'HistoPath_BrCa'),
        ('CancerRegistry', 'HistoPath_OvCa'),
        ('CancerRegistry', 'FlaggingCancers'),
        ('FlaggingCancers', 'HistoPath_BrCa'),
        ('FlaggingCancers', 'HistoPath_OvCa'),

        # ExistingCaSum should be linkable with current sources
        ('CancerRegistry', 'ExistingCaSum'),
        ('ExistingCaSum', 'FlaggingCancers'),
        ('ExistingCaSum', 'HistoPath_BrCa'),
        ('ExistingCaSum', 'HistoPath_OvCa'),

        # Legacy should be linkable with current sources (treated same as ExistingCaSum for linking)
        ('CancerRegistry', 'Legacy'),
        ('FlaggingCancers', 'Legacy'),
        ('HistoPath_BrCa', 'Legacy'),
        ('HistoPath_OvCa', 'Legacy'),
    }

    return sources in linkable_pairs


# helper to safely compare values
def safe_equal(a, b) -> bool:
    """Return True only if both values are non-null and equal."""
    if pd.isna(a) or pd.isna(b):
        return False
    return a == b


# should_link_tumours
def should_link_tumours(anchor: pd.Series, candidate: pd.Series, window: int = 90) -> bool:
    """
    Decide whether two records (anchor, candidate) represent the same tumour.
    Args:
        anchor (Series): Series of data for tumour to compare
        candidate (Series): Series of data for tumour to be compared
        window (integer): duration for diagnosis date match
    Returns: Boolean object
    """
    # same patient
    if anchor["STUDY_ID"] != candidate["STUDY_ID"]:
        return False

    # require both dates present for window check
    if pd.isna(anchor["DIAGNOSIS_DATE"]) or pd.isna(candidate["DIAGNOSIS_DATE"]):
        return False

    date_diff = abs((candidate["DIAGNOSIS_DATE"] - anchor["DIAGNOSIS_DATE"]).days)
    if date_diff > window:
        return False

    # quick check of allowed pair
    if not get_linking_rules(anchor["source"], candidate["source"]):
        return False

    if anchor["source"] in ("ExistingCaSum", "Legacy") or candidate["source"] in ("ExistingCaSum", "Legacy"):
        # identify which is the "new" row and which is the "old"
        if anchor["source"] in ("ExistingCaSum", "Legacy"):
            old_row = anchor.copy()
            new_row = candidate
        else:
            old_row = candidate.copy()
            new_row = anchor
        
        # temporary normalization
        icd = old_row.get("ICD_CODE")
        if isinstance(icd, str) and icd[:3] in {"C56", "C50", "D05"}:
            old_row["ICD_CODE"] = icd[:3]

        # exact-match shortcut across linking fields
        if new_row["source"] in ("CancerRegistry", "HistoPath_BrCa"):
            linking_fields = ["LATERALITY", "ICD_CODE"]
        elif new_row["source"] in ("HistoPath_OvCa"):
            linking_fields = ["ICD_CODE"]
        else:
            linking_fields = ["ICD_CODE", "MORPH_CODE"]

        identical = all(
            (pd.isna(old_row.get(f)) and pd.isna(new_row.get(f))) or safe_equal(old_row.get(f), new_row.get(f))
            for f in linking_fields
        )
        if identical:
            return True

        # Otherwise use same rules (within date window already checked)
        if new_row["source"] in ("CancerRegistry", "HistoPath_BrCa"):
            return safe_equal(old_row.get("ICD_CODE"), new_row.get("ICD_CODE")) and \
                   safe_equal(old_row.get("LATERALITY"), new_row.get("LATERALITY"))
        elif new_row["source"] in ("HistoPath_OvCa"):
            return safe_equal(old_row.get("ICD_CODE"), new_row.get("ICD_CODE"))
        else:
            return safe_equal(old_row.get("ICD_CODE"), new_row.get("ICD_CODE")) and \
                   safe_equal(old_row.get("MORPH_CODE"), new_row.get("MORPH_CODE"))

    # Regular new-vs-new source case
    sources = tuple(sorted([anchor["source"], candidate["source"]]))

    if sources == ('CancerRegistry', 'HistoPath_BrCa'):
        return safe_equal(anchor.get("LATERALITY"), candidate.get("LATERALITY")) and \
               safe_equal(anchor.get("ICD_CODE"), candidate.get("ICD_CODE"))

    elif sources in [('CancerRegistry', 'HistoPath_OvCa'),
                     ('FlaggingCancers', 'HistoPath_OvCa'),
                     ('FlaggingCancers', 'HistoPath_BrCa')]:
        return safe_equal(anchor.get("ICD_CODE"), candidate.get("ICD_CODE"))

    elif sources == ('CancerRegistry', 'FlaggingCancers'):
        return safe_equal(anchor.get("MORPH_CODE"), candidate.get("MORPH_CODE")) and \
               safe_equal(anchor.get("ICD_CODE"), candidate.get("ICD_CODE"))

    return False


# build_clusters_optimized
def build_clusters_optimized(data_sources: dict, window: int = 90) -> List[List[dict]]:
    """
    Build clusters of linked tumours across sources.
    Args:
        data_sources (dictionary): Dictionary with all the data source names and corresponding dataframes
        window (integer): duration for diagnosis date match
    Returns: 
        clusters (list(dict)): Cluster with all the linked tumours in each dictionary
    """

    # --- 1. Gather all records ---
    all_records = []
    for src, df in data_sources.items():
        if df is None or df.empty:
            continue
        for idx, row in df.iterrows():
            all_records.append({
                "source": src,
                "STUDY_ID": row["STUDY_ID"],
                "DIAGNOSIS_DATE": pd.to_datetime(row["DIAGNOSIS_DATE"], errors="coerce"),
                "LATERALITY": row.get("LATERALITY"),
                "MORPH_CODE": row.get("MORPH_CODE"),
                "ICD_CODE": row.get("ICD_CODE"),
                "full_row": row,
                "original_index": idx
            })

    if not all_records:
        return []

    tum_df = pd.DataFrame(all_records)
    tum_df = tum_df.sort_values(["STUDY_ID", "DIAGNOSIS_DATE"]).reset_index(drop=True)

    clusters = []
    visited = set()
    anchor_priority = ["CancerRegistry", "Legacy", "FlaggingCancers", "HistoPath_BrCa", "HistoPath_OvCa"]

    # --- 2. Group by patient ---
    for study_id, group in tum_df.groupby("STUDY_ID"):
        idxs = group.index.tolist()

        for idx in idxs:
            if idx in visited:
                continue

            rec = tum_df.loc[idx]

            # determine priority of current record
            if rec["source"] in anchor_priority:
                rec_pri = anchor_priority.index(rec["source"])
            else:
                rec_pri = len(anchor_priority)

            # skip as anchor only if a higher-priority record exists for same tumour
            higher_priority_exists = False
            for j in idxs:
                if j == idx:
                    continue
                other = tum_df.loc[j]

                if other["source"] in anchor_priority:
                    other_pri = anchor_priority.index(other["source"])
                else:
                    other_pri = len(anchor_priority)

                # Only block if higher-priority AND linkable (could be same tumour)
                if other_pri < rec_pri:
                    if should_link_tumours(other, rec, window):
                        higher_priority_exists = True
                        break

            if higher_priority_exists:
                continue  # skip this anchor

            # anchor is now accepted
            visited.add(idx)
            cluster = [rec]

            # --- 3. Anchor-based linking ---
            for j in idxs:
                if j == idx or j in visited:
                    continue

                candidate = tum_df.loc[j]

                if candidate["source"] in anchor_priority:
                    cand_pri = anchor_priority.index(candidate["source"])
                else:
                    cand_pri = len(anchor_priority)

                if cand_pri >= rec_pri:
                    if should_link_tumours(rec, candidate, window):
                        cluster.append(candidate)
                        visited.add(j)

            # --- 4. Format cluster ---
            formatted = []
            for rec_item in cluster:
                formatted.append({
                    "source": rec_item["source"],
                    "row": rec_item["full_row"],
                    "STUDY_ID": rec_item["STUDY_ID"],
                    "DIAGNOSIS_DATE": rec_item["DIAGNOSIS_DATE"],
                    "LATERALITY": rec_item["LATERALITY"],
                    "MORPH_CODE": rec_item["MORPH_CODE"],
                    "ICD_CODE": rec_item["ICD_CODE"]
                })

            clusters.append(formatted)

    return clusters


# select_value_per_field
def select_value_per_field(cluster_matches, target_schema, default_source="CancerRegistry"):
    """
    Field value selection respecting per-field x-sourcePriority
    Args:
        cluster_matches (dictionary): rows from matching sources
        target_schema (dictionary): schema of the result dataset
    Returns: 
        target_row (dict): Tumour row selected from the cluster
    """

    global_priority = [
        "CancerRegistry",
        "FlaggingCancers",
        "Legacy",
        "HistoPath_BrCa",
        "HistoPath_OvCa"
    ]

    target_row = {}

    # normalize ExistingCaSum
    old_row = cluster_matches.get("ExistingCaSum")
    if isinstance(old_row, pd.Series):
        old_row = old_row.to_dict()
    elif old_row is None:
        old_row = {}

    for var, details in target_schema["properties"].items():
        if var.startswith("S_"):
            continue

        val = None
        src_used = None

        # --- Build effective priority order ---
        fld_priority = details.get("x-sourcePriority", [])
        if fld_priority:
            ordered_sources = []
            col_map = {}
            for src_field in fld_priority:
                try:
                    src_name, col_name = src_field.split(".")
                except ValueError:
                    continue
                ordered_sources.append(src_name)
                col_map[src_name] = col_name

            # inject Legacy after FlaggingCancers if not already there
            if "Legacy" not in ordered_sources:
                if "FlaggingCancers" in ordered_sources:
                    idx = ordered_sources.index("FlaggingCancers")
                    ordered_sources.insert(idx, "Legacy")
                else:
                    # if FlaggingCancers missing, put after CancerRegistry if present
                    if "CancerRegistry" in ordered_sources:
                        idx = ordered_sources.index("CancerRegistry") + 1
                        ordered_sources.insert(idx, "Legacy")
                    else:
                        ordered_sources.insert(0, "Legacy")
        else:
            ordered_sources = list(global_priority)
            col_map = {}

        # --- Iterate by priority and select first available value ---
        for src in ordered_sources:
            if src not in cluster_matches:
                continue
            col = col_map.get(src, var)
            candidate_row = cluster_matches[src]
            candidate_val = candidate_row.get(col) if hasattr(candidate_row, "get") else None

            if pd.notna(candidate_val) and candidate_val != "":
                val = candidate_val
                if src == "Legacy":
                    # keep original Legacy provenance (e.g., 'PHE', 'ENCORE')
                    src_used = candidate_row.get(f"S_{var}", None)
                else:
                    src_used = f"{src}.{col}"
                break

        # --- Fallback to ExistingCaSum if no new value ---
        if val is None and old_row:
            val = old_row.get(var)
            src_used = old_row.get(f"S_{var}", None)

        target_row[var] = val
        if f"S_{var}" in target_schema["properties"]:
            target_row[f"S_{var}"] = src_used

    return target_row