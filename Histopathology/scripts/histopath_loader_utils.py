import sys
import os
import json
import pandas as pd

sys.path.append(os.path.abspath(r"N:\CancerEpidem\BrBreakthrough\DeliveryProcess\Schema_and_Derivation_utils"))
from utilities import connect_DB, read_data


# ------------------------------------------------------------
# Helpers: read schema and get table->fields mapping
# ------------------------------------------------------------

def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def schema_fields_for_table(schema: dict) -> list[str]:
    """
    Returns the flat list of schema-defined fields for Histopath_BrCa.
    The histopathology schema is a single flat object, so the fields are
    taken directly from schema['properties'] in schema order.
    """
    return list(schema.get("properties", {}).keys())


# ------------------------------------------------------------
# Helpers: compare loaded SQL columns to schema fields
# ------------------------------------------------------------

def keep_schema_matching_columns(df: pd.DataFrame, schema_fields: list[str]):
    """
    Keep only columns that appear in the schema, preserving schema order.

    Returns:
      - filtered dataframe
      - matched_schema_fields
      - missing_schema_fields
      - extra_sql_fields
    """
    loaded_columns = list(df.columns)
    loaded_set = set(loaded_columns)

    matched_schema_fields = [field for field in schema_fields if field in loaded_set]
    missing_schema_fields = [field for field in schema_fields if field not in loaded_set]
    extra_sql_fields = [field for field in loaded_columns if field not in set(schema_fields)]

    filtered_df = df.loc[:, matched_schema_fields].copy()
    return filtered_df, matched_schema_fields, missing_schema_fields, extra_sql_fields


# ------------------------------------------------------------
# Core: pull Histopath_BrCa_GS_v1 into a df
# ------------------------------------------------------------

def load_histopath_df(
    schema_path: str,
    server: str,
    logger,
    pathology_db: str = "UpLoads",
    table_name: str = "Histopath_BrCa_GS_v1",
    dbo_schema: str = "dbo",
):
    schema = load_json(schema_path)
    schema_fields = schema_fields_for_table(schema)

    conn = connect_DB(pathology_db, server, logger)

    query = f"""
        SELECT *
        FROM [{pathology_db}].[{dbo_schema}].[{table_name}]
    """

    df_raw = read_data(query, conn, logger)
    df, matched_schema_fields, missing_schema_fields, extra_sql_fields = keep_schema_matching_columns(
        df_raw,
        schema_fields,
    )

    logger.info(f"Loaded SQL columns: {len(df_raw.columns):,}")
    logger.info(f"Matched schema columns kept: {len(matched_schema_fields):,}")

    if missing_schema_fields:
        logger.warning(
            "Schema fields not found in SQL table and therefore excluded: "
            + ", ".join(missing_schema_fields)
        )

    if extra_sql_fields:
        logger.info(
            "SQL columns not present in schema and therefore dropped after loading: "
            + ", ".join(extra_sql_fields)
        )

    return schema, df, missing_schema_fields
