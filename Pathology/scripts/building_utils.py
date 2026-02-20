import sys
import os
import re
import pandas as pd
import numpy as np
from datetime import datetime, date, time as dtime

sys.path.append(os.path.abspath("N:\\CancerEpidem\\BrBreakthrough\\DeliveryProcess\\Schema_and_Derivation_utils\\Pathology\\scripts"))
from loader_utils import schema_fields_for_table


def make_json_safe(x):
    """Recursively convert pandas/numpy types to JSON-serialisable Python primitives.

    - NaN/NaT -> None
    - pandas.Timestamp/datetime/date -> ISO strings
    - numpy scalars -> python scalars
    - dict/list -> recurse
    """
    # nulls / NaT
    if x is None:
        return None
    if isinstance(x, (pd.Timestamp, datetime)) and pd.isna(x):
        return None
    if isinstance(x, float) and pd.isna(x):
        return None
    if isinstance(x, np.generic) and pd.isna(x):
        return None

    # pandas Timestamp -> python datetime
    if isinstance(x, pd.Timestamp):
        x = x.to_pydatetime()

    # python datetime -> ISO string
    if isinstance(x, datetime):
        if x.time() == dtime(0, 0, 0):
            return x.date().isoformat()
        return x.isoformat()

    # python date -> ISO string
    if isinstance(x, date):
        return x.isoformat()

    # numpy scalars -> python scalars
    if isinstance(x, np.generic):
        return x.item()

    # recurse
    if isinstance(x, dict):
        return {k: make_json_safe(v) for k, v in x.items()}
    if isinstance(x, list):
        return [make_json_safe(v) for v in x]

    return x

def _df_to_records(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []
    # Convert NaN/NaT -> None and coerce non-JSON-native objects (e.g., Timestamps)
    records = df.where(pd.notnull(df), None).to_dict("records")
    return [make_json_safe(r) for r in records]

def build_nested_breast_tumour_link(schema: dict, df_tt: pd.DataFrame, df_lt: pd.DataFrame, df_tma: pd.DataFrame) -> list[dict]:
    """
    Schema-aligned output:
    [
      {"StudyID": 123456, "TumourTracking": [ { ... , "LabTracking":[...], "TMAs":[...] }, ... ]},
      ...
    ]

    IMPORTANT: BreastTumourLink_Schema.json does NOT include StudyID inside TumourTracking,
    so StudyID often won't be selected from SQL. We therefore derive StudyID from:
      1) df_tt['StudyID'] if present, otherwise
      2) first 6-digit sequence found in LabID.
    """

    tt_props = schema["properties"]["TumourTracking"]["items"]["properties"]
    tumour_fields = [k for k in tt_props.keys() if k not in ("LabTracking", "TMAs")]
    lab_fields = list(tt_props["LabTracking"]["items"]["properties"].keys())
    tma_fields = list(tt_props["TMAs"]["items"]["properties"].keys())

    def _only_schema_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=cols)
        present = [c for c in cols if c in df.columns]
        out = df[present].copy()
        for c in cols:
            if c not in out.columns:
                out[c] = None
        return out[cols]

    def _clean_records(df: pd.DataFrame) -> list[dict]:
        if df is None or df.empty:
            return []
        records = df.where(pd.notnull(df), None).to_dict("records")
        return [make_json_safe(r) for r in records]

    # Keep only schema columns (TumourTracking schema does not include StudyID)
    df_tt = _only_schema_cols(df_tt, tumour_fields)
    df_lt = _only_schema_cols(df_lt, lab_fields)
    df_tma = _only_schema_cols(df_tma, tma_fields)

    # ---- Recode BlockSide: 'l' -> 'L' (leave everything else unchanged) ----
    if "BlockSide" in df_tt.columns:
        df_tt["BlockSide"] = df_tt["BlockSide"].apply(
            lambda x: "L" if isinstance(x, str) and x.strip() == "l" else x
        )

    # Normalize LabNo for joins
    for df in (df_tt, df_lt, df_tma):
        if "LabNo" in df.columns:
            df["LabNo"] = pd.to_numeric(df["LabNo"], errors="coerce")

    # --- Derive StudyID per tumour row ---
    # If StudyID is not present (common in your current pipeline), parse it from LabID.
    if "StudyID" in df_tt.columns:
        df_tt["_StudyID_for_grouping"] = pd.to_numeric(df_tt["StudyID"], errors="coerce")
    else:
        def _parse_studyid_from_labid(x):
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return None
            s = str(x)
            m = re.search(r"\b(\d{6})\b", s)  # first standalone 6-digit sequence
            return int(m.group(1)) if m else None

        if "LabID" in df_tt.columns:
            df_tt["_StudyID_for_grouping"] = df_tt["LabID"].apply(_parse_studyid_from_labid)
        else:
            # Worst case: cannot derive StudyID at all
            df_tt["_StudyID_for_grouping"] = None

    # Index nested tables by LabNo
    lt_by_labno = {}
    if not df_lt.empty and "LabNo" in df_lt.columns:
        for labno, g in df_lt.groupby("LabNo", dropna=True):
            lt_by_labno[labno] = _clean_records(g)

    tma_by_labno = {}
    if not df_tma.empty and "LabNo" in df_tma.columns:
        for labno, g in df_tma.groupby("LabNo", dropna=True):
            tma_by_labno[labno] = _clean_records(g)

    # Build TumourTracking rows (include derived grouping StudyID only for grouping, not output)
    tumour_rows = []
    if not df_tt.empty:
        df_tt_clean = df_tt.where(pd.notnull(df_tt), None)
        for row in df_tt_clean.to_dict("records"):
            labno = row.get("LabNo")
            row["LabTracking"] = lt_by_labno.get(labno, []) if labno is not None else []
            row["TMAs"] = tma_by_labno.get(labno, []) if labno is not None else []
            row = make_json_safe(row)
            tumour_rows.append(row)

    # Group under StudyID using the derived column
    by_sid = {}
    for tr in tumour_rows:
        sid = tr.get("_StudyID_for_grouping")
        # remove helper field from tumour rows (not in schema)
        tr.pop("_StudyID_for_grouping", None)
        by_sid.setdefault(sid, []).append(tr)

    return make_json_safe([{"StudyID": sid, "TumourTracking": tumours} for sid, tumours in by_sid.items()])