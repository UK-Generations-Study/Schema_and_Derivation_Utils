import sys
import os
import json
import pandas as pd

sys.path.append(os.path.abspath("N:\\CancerEpidem\\BrBreakthrough\\DeliveryProcess\\Schema_and_Derivation_utils"))
from utilities import connect_DB, read_data
from utilities import createLogger

# ------------------------------------------------------------
# Helpers: read schema and get table->fields mapping
# ------------------------------------------------------------
def load_json(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)

def schema_fields_for_table(schema: dict) -> dict:
    """
    Returns dict with the schema-defined fields for:
      - top level StudyID
      - TumourTracking fields
      - LabTracking fields (nested under TumourTracking)
      - TMAs fields (nested under TumourTracking)
    Based on BreastTumourLink.json layout. :contentReference[oaicite:3]{index=3}
    """
    tt_props = schema["properties"]["TumourTracking"]["items"]["properties"]

    tumour_fields = [k for k in tt_props.keys() if k not in ("LabTracking", "TMAs")]
    lab_fields = list(tt_props["LabTracking"]["items"]["properties"].keys())
    tma_fields = list(tt_props["TMAs"]["items"]["properties"].keys())

    return {
        "top": ["StudyID"],
        "TumourTracking": tumour_fields,
        "LabTracking": lab_fields,
        "TMAMapping": tma_fields,
    }

def sql_select_list(fields: list[str]) -> str:
    # bracket-quote fields safely (handles "ER%Positive")
    return ", ".join(f"[{f}]" for f in fields)

# ------------------------------------------------------------
# Core: pull each table into a df
# ------------------------------------------------------------
def load_pathology_dfs(
    schema_path: str,
    server: str,
    logger,
    pathology_db: str = "Pathology",
    dbo_schema: str = "dbo",
):
    schema = load_json(schema_path)
    fields = schema_fields_for_table(schema)

    conn = connect_DB(pathology_db, server, logger)

    # --- TumourTracking is the "parent" grain (unique by LabNo) ---
    tt_cols = fields["TumourTracking"]
    tt_query = f"""
        SELECT {sql_select_list(tt_cols)}
        FROM [{pathology_db}].[{dbo_schema}].[TumourTracking]
    """

    # --- LabTracking is nested under TumourTracking via LabNo ---
    lt_cols = fields["LabTracking"]
    lt_query = f"""
        SELECT {sql_select_list(lt_cols)}
        FROM [{pathology_db}].[{dbo_schema}].[LabTracking]
    """

    # --- TMAs_All is nested under TumourTracking via LabNo ---
    tma_cols = fields["TMAMapping"]
    tma_query = f"""
        SELECT {sql_select_list(tma_cols)}
        FROM [{pathology_db}].[{dbo_schema}].[TMAMapping]
    """

    df_tt  = read_data(tt_query,  conn, logger)
    df_lt  = read_data(lt_query,  conn, logger)
    df_tma = read_data(tma_query, conn, logger)

    return schema, df_tt, df_lt, df_tma