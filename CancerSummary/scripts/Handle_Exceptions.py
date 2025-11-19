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


def resolve_morph_code_conflicts(df: pd.DataFrame, sources) -> pd.DataFrame:
    """
    Resolves MORPH_CODE conflicts between 2 sources.
    Fully compatible with multi-row groups and NaN handling.

    Logic:
      1. Group by STUDY_ID, DIAGNOSIS_DATE, or LATERALITY depending pn the sources.
      2. If CANCER_SITE differs → keep all.
      3. If same → apply per-pair logic:
         - If one MORPH_CODE is null → keep the non-null.
         - If both contain '8500':
             keep one ending with '3' if available, else Registry.
         - If only one contains '8500' → keep the other.
         - Else → prefer Registry.
      4. If multiple Registry/HistoPath pairs → evaluate each pair.
      5. Remove all losing rows from that group.
    """

    df = df.copy()
    df["_orig_index"] = df.index
    to_drop = set()
    new_rows = []

    def morph_to_str(m):
        """Convert numeric/float morph safely to string; return '' for NaN/empty."""
        if pd.isna(m):
            return ""
        # preserve strings as-is, but remove .0 if coming from float-like ints
        try:
            # if it's a float that's effectively an integer, cast to int first
            if isinstance(m, float) and m.is_integer():
                return str(int(m))
            return str(m).strip()
        except Exception:
            return str(m)

    if set(sources) == {"CancerRegistry_0125", "HistoPath_BrCa"}:
        grouped = df.groupby(["STUDY_ID", "DIAGNOSIS_DATE", "LATERALITY"])
    else:
        grouped = df.groupby(["STUDY_ID", "DIAGNOSIS_DATE"])

    for _, group in grouped:
        # single-row group -> keep
        if len(group) == 1:
            new_rows.append(group.iloc[0])
            continue

        # If cancer sites differ, do not resolve (keep all)
        if group["CANCER_SITE"].nunique(dropna=True) > 1:
            new_rows.extend(group.to_dict("records"))
            continue

        # Separate relevant sources and others
        candidates = group[group["S_STUDY_ID"].isin(sources)].copy()
        others = group[~group["S_STUDY_ID"].isin(sources)].to_dict("records")

        # If no candidates or only one type present -> keep as-is
        if candidates.empty or len(candidates) == 1:
            new_rows.extend(group.to_dict("records"))
            continue

        # Build ranking keys for each candidate
        ranks = []
        for idx, row in candidates.iterrows():
            morph_raw = row.get("MORPH_CODE")
            morph_s = morph_to_str(morph_raw)

            is_null = (morph_s == "")
            has_8500 = ("8500" in morph_s)
            ends_with_3 = morph_s.endswith("3") if morph_s else False
            length_digits = len(morph_s)

            # Source priority: prefer CancerRegistry if tie
            source_priority = 1 if row["S_STUDY_ID"] == "CancerRegistry_0125" else 0

            # Ranking tuple (higher is better); tuple ordered by importance:
            # 1) non-null (1/0), 2) no-8500 (1/0), 3) endswith3 (1/0),
            # 4) length_digits (int), 5) source_priority (1/0)
            rank_tuple = (
                0 if is_null else 1,          # prefer non-null
                0 if has_8500 else 1,         # prefer not containing 8500
                1 if ends_with_3 else 0,      # prefer ending with 3
                length_digits,                # prefer longer (more detailed)
                source_priority               # prefer registry if tie
            )

            ranks.append((idx, rank_tuple, row))

        # If all morphs are null, keep CancerRegistry if present else first candidate
        if all(r[1][0] == 0 for r in ranks):
            # find registry candidate if exists
            reg_candidates = [r for r in ranks if r[2]["S_STUDY_ID"] == "CancerRegistry_0125"]
            if reg_candidates:
                chosen_idx, _, chosen_row = reg_candidates[0]
            else:
                chosen_idx, _, chosen_row = ranks[0]
            new_rows.append(chosen_row)
            # mark all other candidate rows for dropping
            for idx, _, row in ranks:
                if idx != chosen_idx:
                    to_drop.add(row["_orig_index"])
            # add 'others' back
            new_rows.extend(others)
            continue

        # choose candidate with max rank_tuple lexicographically
        # stable deterministic: if multiple equal, keep first occurrence of max
        ranks_sorted = sorted(ranks, key=lambda x: x[1], reverse=True)
        chosen_idx, chosen_rank, chosen_row = ranks_sorted[0]

        # append chosen row
        new_rows.append(chosen_row)

        # mark other candidate rows for dropping
        for idx, _, row in ranks:
            if idx != chosen_idx:
                to_drop.add(row["_orig_index"])

        # keep other sources untouched
        new_rows.extend(others)

    # Keep unprocessed rows (not dropped)
    untouched = df[~df['_orig_index'].isin(to_drop)].copy()
    
    cleaned = [dict(row) for row in new_rows if isinstance(row, (dict, pd.Series))]

    # Combine resolved + untouched
    final_df = pd.DataFrame(cleaned).drop_duplicates(subset=['_orig_index'], keep="first")
    combined = pd.concat([final_df, untouched[~untouched['_orig_index'].isin(final_df['_orig_index'])]], ignore_index=True)
    
    combined = combined.drop(columns=['_orig_index'])

    return combined