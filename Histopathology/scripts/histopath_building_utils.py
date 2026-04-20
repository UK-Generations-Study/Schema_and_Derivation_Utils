import sys
import os
from datetime import datetime, date, time as dtime
import json
import hashlib

import numpy as np
import pandas as pd

sys.path.append(os.path.abspath(r"N:\CancerEpidem\BrBreakthrough\DeliveryProcess\Schema_and_Derivation_utils\Histopathology\scripts"))
from histopath_loader_utils import schema_fields_for_table
from histopath_map_and_derive import harmonize_source, get_stage

sys.path.append(os.path.abspath("N:\\CancerEpidem\\BrBreakthrough\\DeliveryProcess\\Schema_and_Derivation_utils"))
from config import brca_variables_to_map, brca_special_rules, stage_rules, live_server
from utilities import createLogger, read_data, connect_DB

# ------------------------------------------------------------
# JSON-safe converters
# ------------------------------------------------------------

def make_json_safe(x):
    """Recursively convert pandas/numpy types to JSON-serialisable Python primitives."""
    if x is None:
        return None
    if isinstance(x, (pd.Timestamp, datetime)) and pd.isna(x):
        return None
    if isinstance(x, float) and pd.isna(x):
        return None
    if isinstance(x, np.generic) and pd.isna(x):
        return None

    if isinstance(x, pd.Timestamp):
        x = x.to_pydatetime()

    if isinstance(x, datetime):
        if x.time() == dtime(0, 0, 0):
            return x.date().isoformat()
        return x.isoformat()

    if isinstance(x, date):
        return x.isoformat()

    if isinstance(x, np.generic):
        return x.item()

    if isinstance(x, dict):
        return {k: make_json_safe(v) for k, v in x.items()}
    if isinstance(x, list):
        return [make_json_safe(v) for v in x]

    return x


def _clean_datetime_like_columns(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    out = df.copy()
    props = schema.get("properties", {})
    for col, meta in props.items():
        if col not in out.columns or not isinstance(meta, dict):
            continue
        if meta.get("format") == "date-time":
            out[col] = pd.to_datetime(out[col], errors="coerce")
    return out


def _only_schema_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=cols)

    present = [c for c in cols if c in df.columns]
    out = df[present].copy()
    for c in cols:
        if c not in out.columns:
            out[c] = None
    return out[cols]

def _first_non_null(series_list: list[pd.Series]) -> pd.Series:
    """Return first non-null value across aligned Series objects."""
    if not series_list:
        return pd.Series(dtype='object')

    out = series_list[0].copy()
    for s in series_list[1:]:
        out = out.where(out.notna(), s)
    return out


def _sum_nullable_int_columns(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    """
    Sum integer-like columns while preserving null when all inputs are null.
    Treat nulls as 0 only if at least one component is non-null.
    """
    available = [c for c in cols if c in df.columns]
    if not available:
        return pd.Series([pd.NA] * len(df), index=df.index, dtype='Int64')

    numeric = df[available].apply(pd.to_numeric, errors='coerce')
    has_any_value = numeric.notna().any(axis=1)
    summed = numeric.fillna(0).sum(axis=1)
    return summed.where(has_any_value, pd.NA).astype('Int64')


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


# ------------------------------------------------------------
# Build schema-aligned flat records
# ------------------------------------------------------------

def build_histopath_records(schema: dict, df: pd.DataFrame, logger) -> list[dict]:
    """
    Build a flat schema-aligned list[dict] for Histopath_BrCa.
    Mirrors the pathology pipeline structure, but histopathology is a single
    flat table rather than a nested tumour/lab/TMA hierarchy.
    """
    cols = schema_fields_for_table(schema)
    df = _only_schema_cols(df, cols)
    df = _clean_datetime_like_columns(df, schema)

    if "StudyID" in df.columns:
        df["StudyID"] = df["StudyID"].apply(
            lambda x: int(x)
        )

    hist_Brca_schema_path = r"N:\CancerEpidem\BrBreakthrough\DeliveryProcess\Schema_and_Derivation_utils\Histopathology\schemas\raw"
    target_schema_path = r"N:\CancerEpidem\BrBreakthrough\DeliveryProcess\Schema_and_Derivation_utils\CancerSummary\schemas"

    with open(os.path.join(hist_Brca_schema_path, "HistoPath_BrCa.json"), 'r') as schema:
        hist_Brca_schema = json.load(schema)

    with open(os.path.join(target_schema_path, "NewCancerSummary.json"), 'r') as schema:
        target_schema = json.load(schema)


    # Run harmonization for path breast source
    brca_mapped, mappings_used = harmonize_source(
        df, hist_Brca_schema, target_schema, brca_variables_to_map, logger, brca_special_rules
    )

    for src_var, info in mappings_used.items():    
        logger.info("Source column:" + str(src_var))
        logger.info("Rows mapped:" + str(info['changed_rows']))
        logger.info("Mapping dictionary used:" + str(info['mapping']))

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
    lookup_df = pd.DataFrame(stage_rules)

    # Convert to dictionary for fast lookup
    lookup_dict = lookup_df.set_index("StagePattern").to_dict(orient="index")
    patterns = lookup_df["StagePattern"].tolist()

    logger.info("Deriving the Stage variable for HistoPath Breast data")
    brca_mapped["Stage"] = brca_mapped.apply(get_stage, axis=1, args=(patterns, lookup_dict))
   
    logger.info("Deriving GRADE, TUMOUR_SIZE, and NODES_TOTAL for breast histopathology data")

    # GRADE: prefer invasive grade, otherwise use DCIS grade
    brca_mapped['GRADE'] = _first_non_null([
        brca_mapped['InvasiveGrade'],
        brca_mapped['DCISGrade']
    ])

    # TUMOUR_SIZE: prefer invasive tumour size, otherwise use DCIS-only size
    brca_mapped['TUMOUR_SIZE'] = _first_non_null([
        pd.to_numeric(brca_mapped['SizeInvasiveTumour'], errors='coerce'),
        pd.to_numeric(brca_mapped['SizeDCISOnly'], errors='coerce')
    ])

    # NODES_TOTAL: sum axillary and other nodes; keep null if both are missing
    brca_mapped['NODES_TOTAL'] = _sum_nullable_int_columns(
        brca_mapped, ['AxillaryNodesTotal', 'OtherNodesTotal']
    )

    # NODES_POSITIVE: sum axillary and other positive nodes; keep null if both are missing
    brca_mapped['NODES_POSITIVE'] = _sum_nullable_int_columns(
        brca_mapped, ['AxillaryNodesPositive', 'OtherNodesPositive']
    )

    brca_mapped['ICDMorphologyCode'] = brca_mapped['ICDMorphologyCode'].str.replace("M", "",)
    
    mailing_conn = connect_DB('Mailing', live_server, logger)

    logger.info('Reading People table to map PersonID to StudyID and to get the Date of Birth')
    people = read_data('select PersonID, StudyID, cast(DATEFROMPARTS(DOBYear, DOBMonth, DOBDay) as date)as DOB \
                    from People', mailing_conn, logger)

    brca_mapped = brca_mapped.merge(people, left_on=['StudyID'], right_on=['StudyID'], how='left')

    if "AGE_AT_DIAGNOSIS" not in brca_mapped.columns:
        brca_mapped['AGE_AT_DIAGNOSIS'] = pd.NA

    brca_mapped["DiagDat"] = pd.to_datetime(brca_mapped["DiagDat"], errors="coerce")
    brca_mapped["DOB"] = pd.to_datetime(brca_mapped["DOB"], errors="coerce")

    brca_mapped['AGE_AT_DIAGNOSIS'] = np.where(brca_mapped['AGE_AT_DIAGNOSIS'].isna(), \
                                    ((brca_mapped['DiagDat'] - brca_mapped['DOB']).dt.days // 365.25),\
                                    brca_mapped['AGE_AT_DIAGNOSIS'])

    brca_mapped['AGE_AT_DIAGNOSIS'] = pd.to_numeric(brca_mapped['AGE_AT_DIAGNOSIS'], errors='coerce').astype('Int64')

    # Common recodes / normalisation seen in pathology-style builders
    if "Side" in brca_mapped.columns:
        brca_mapped['Side'] = brca_mapped['Side'].replace("l","L")

    if "AxillaryNodesPresent" in brca_mapped.columns:
        brca_mapped['AxillaryNodesPresent'] = brca_mapped['AxillaryNodesPresent'].replace("y", "Y",)

    if "InvasiveGrade" in brca_mapped.columns:
        brca_mapped['InvasiveGrade'] = brca_mapped['InvasiveGrade'].replace("1 ", "1")

    if "PR_Status" in brca_mapped.columns:
        brca_mapped['PR_Status'] = brca_mapped['PR_Status'].replace("p", "P")
    
    # update HER2Score to higher end value
    if "HER2_Score" in brca_mapped.columns:
        brca_mapped['HER2_Score'] = brca_mapped['HER2_Score'].apply(getHighestMarker) 

    records = brca_mapped.where(pd.notnull(brca_mapped), None).to_dict("records")
    return [make_json_safe(r) for r in records]
