# pseudo_anon_histopath.py

import sys
import os
import copy
import json
from datetime import datetime
from collections import OrderedDict

import pandas as pd

from histopath_map_and_derive import build_enum_mapping
from histopath_building_utils import make_json_safe

sys.path.append(os.path.abspath(
    r"N:\CancerEpidem\BrBreakthrough\DeliveryProcess\Schema_and_Derivation_utils\Questionnaire\common_scripts"
))
from pseudo_anon_utils import load_sid_codes, pseudo_anonymize_studyid  # keep shared utils

sys.path.append(os.path.abspath(
    r"N:\CancerEpidem\BrBreakthrough\DeliveryProcess\Schema_and_Derivation_utils"
))
from config import brca_variables_to_map, brca_special_rules



# -----------------------------
# Histopath pseudo-anon rules
# -----------------------------
BASE_DROP_FIELDS = {
    "ReportDat",
    "CoreDat",
    "DateSpecimenTaken",
    "DateSpecimenReceived",
}

PROCESSING_ONLY_DROP_FIELDS = {
    "TstageMapped",
    "TstageDer",
    "NStageMapped",
    "NStageDer",
    "MStageMapped",
    "MStageDer",
    "DOB",
    "PersonID",
    "AxillaryNodesTotal",
    "AxillaryNodesPositive",
    "OtherNodesTotal",
    "OtherNodesPositive",
    "SizeInvasiveTumour",
}

RENAME_MAP = {
    "DiagDat": "DIAGNOSIS_DATE_SHIFTED",
    "Side": "LATERALITY",
    "ReportCount": "REPORT_COUNT",
    "TumourCount": "TUMOUR_COUNT",
    "NStage": "N_STAGE",
    "Stage": "STAGE",  
}

FORMER_NAME_MAP = {
    "TCode": "StudyID",
    "DIAGNOSIS_DATE_SHIFTED": "DiagDat",
    "LATERALITY": "Side",
    "REPORT_COUNT": "ReportCount",
    "TUMOUR_COUNT": "TumourCount",
    "STAGE": "Stage",  
}




DATE_SHIFT_SOURCE_POINTER = "#/properties/DiagDat"

MAPPED_SOURCE_DROP_FIELDS = set(brca_variables_to_map.keys())

DROP_FIELDS = (
    BASE_DROP_FIELDS
    | PROCESSING_ONLY_DROP_FIELDS
    | MAPPED_SOURCE_DROP_FIELDS
)


# -----------------------------
# Schema metadata helpers
# -----------------------------

def _prefix_schema_title(title: str) -> str:
    if not isinstance(title, str):
        return title
    prefix = "Pseudo Anonymised "
    return title if title.startswith(prefix) else f"{prefix}{title}"


def _append_pseudoanon_sentence(description: str) -> str:
    sentence = (
        "This data has been pseudo-anonymised by removing any personal identifiable "
        "information (PII) and shifting dates from their original data."
    )
    if not isinstance(description, str) or not description.strip():
        return sentence
    return description if sentence in description else f"{description} {sentence}"


def _update_schema_metadata(schema: dict) -> dict:
    if not isinstance(schema, dict):
        return schema

    schema_id = schema.get("$id")
    if isinstance(schema_id, str):
        if schema_id.endswith("_PseudoAnon.json"):
            schema["$id"] = schema_id
        elif schema_id.endswith(".json"):
            schema["$id"] = schema_id[:-5] + "_PseudoAnon.json"
        else:
            schema["$id"] = schema_id + "_PseudoAnon"

    if "title" in schema:
        schema["title"] = _prefix_schema_title(schema["title"])

    schema["description"] = _append_pseudoanon_sentence(schema.get("description"))

    if isinstance(schema.get("x-provenance"), dict):
        schema["x-provenance"]["x-lastModified"] = datetime.today().strftime("%Y-%m-%d")

    return schema


def _copy_title_annotations(source_field: dict, target_field: dict) -> dict:
    if not isinstance(target_field, dict):
        return target_field

    for key in ("title", "x-title", "x-displayName", "x-shortTitle"):
        if isinstance(source_field, dict) and key in source_field and key not in target_field:
            target_field[key] = copy.deepcopy(source_field[key])
    return target_field


def _copy_common_annotations(source_field: dict, target_field: dict) -> dict:
    if not isinstance(target_field, dict):
        return target_field
    if not isinstance(source_field, dict):
        return target_field

    for key in (
        "title",
        "x-title",
        "x-displayName",
        "x-shortTitle",
        "x-unit",
        "minimum",
        "maximum",
        "minLength",
        "maxLength",
        "format",
        "pattern",
    ):
        if key in source_field and key not in target_field:
            target_field[key] = copy.deepcopy(source_field[key])
    return target_field


def _add_or_update(d: dict, key: str, value):
    if isinstance(d, dict):
        d[key] = value


def _rename_field_name(field_name: str) -> str:
    if field_name == "StudyID":
        return "TCode"
    return RENAME_MAP.get(field_name, field_name)


def _remap_primary_key(pk):
    if isinstance(pk, list):
        out = []
        for item in pk:
            if item in DROP_FIELDS:
                continue
            out.append(_rename_field_name(item))
        return out
    if isinstance(pk, str):
        return _rename_field_name(pk)
    return pk


# -----------------------------
# Data helpers
# -----------------------------

def _records_to_df(data: list[dict]) -> pd.DataFrame:
    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data)


def _df_to_records(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []
    return df.where(pd.notnull(df), None).to_dict("records")

def _format_full_iso_timestamp(value):
    if value is None:
        return None

    try:
        ts = pd.to_datetime(value, errors="coerce")
        if pd.isna(ts):
            return None

        # ensure UTC
        ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")

        # no milliseconds
        return ts.strftime("%Y-%m-%dT%H:%M:%SZ")

    except Exception:
        return None

def _walk_and_rename_keys(obj, mapping: dict):
    if isinstance(obj, dict):
        for k in list(obj.keys()):
            v = obj[k]
            if k in mapping:
                new_k = mapping[k]
                obj[new_k] = v
                del obj[k]
                _walk_and_rename_keys(obj.get(new_k), mapping)
            else:
                _walk_and_rename_keys(v, mapping)
    elif isinstance(obj, list):
        for item in obj:
            _walk_and_rename_keys(item, mapping)


def _shift_diagdat_from_sid_codes(df: pd.DataFrame, sid_df: pd.DataFrame, logger=None) -> pd.DataFrame:
    out = df.copy()

    out = out.merge(
        sid_df[["StudyID", "Random"]].drop_duplicates(),
        on="StudyID",
        how="left",
    )

    out["DiagDat"] = pd.to_datetime(out["DiagDat"], errors="coerce")
    out["DiagDat"] = out["DiagDat"] + pd.to_timedelta(out["Random"], unit="D")
    out = out.drop(columns=["Random"], errors="ignore")

    return out


# -----------------------------
# Main: apply pseudo-anon + field filtering
# -----------------------------
def _move_tcode_first(record: dict) -> dict:
    if not isinstance(record, dict) or "TCode" not in record:
        return record

    return {"TCode": record["TCode"], **{k: v for k, v in record.items() if k != "TCode"}}


def _coerce_morph_code(value):
    if value is None:
        return None

    # handle things like "8500/3"
    if isinstance(value, str):
        import re
        match = re.search(r"\d+", value)
        if match:
            return match.group()

    try:
        return value
    except (ValueError, TypeError):
        return None


def apply_histopath_privacy_transforms(
    data: list[dict],
    server: str,
    logger,
) -> list[dict]:
    df = _records_to_df(data)
    sid_df = load_sid_codes(server, logger)

    required_cols = {"StudyID", "TCode"}
    missing_required = required_cols - set(sid_df.columns)
    if missing_required:
        raise ValueError(
            f"SID code mapping missing required columns: {sorted(missing_required)}"
        )

    df = _shift_diagdat_from_sid_codes(df, sid_df, logger=logger)

    df['NODES_POSITIVE'] = pd.to_numeric(df['NODES_POSITIVE'], errors='coerce').astype('Int64')
    df['NODES_TOTAL'] = pd.to_numeric(df['NODES_TOTAL'], errors='coerce').astype('Int64')
    
    records = _df_to_records(df)

    records = pseudo_anonymize_studyid(records, sid_df)

    for rec in records:
        if not isinstance(rec, dict):
            continue
        for field in DROP_FIELDS:
            rec.pop(field, None)

    _walk_and_rename_keys(records, RENAME_MAP)

    
    # enforce types before JSON conversion
    for rec in records:
        if not isinstance(rec, dict):
            continue

        # MORPH_CODE → to handle 8500/3
        if "ICDMorphologyCode" in rec:
            rec["ICDMorphologyCode"] = _coerce_morph_code(rec.get("ICDMorphologyCode"))

        # DIAGNOSIS_DATE_SHIFTED → full ISO string
        if "DIAGNOSIS_DATE_SHIFTED" in rec:
            rec["DIAGNOSIS_DATE_SHIFTED"] = _format_full_iso_timestamp(
                rec.get("DIAGNOSIS_DATE_SHIFTED")
            )

    # final JSON-safe conversion
    return [
        _move_tcode_first(make_json_safe(r))
        for r in records
    ]


# -----------------------------
# Pipeline-aware schema helpers
# -----------------------------

def _safe_get_property(schema: dict, field: str) -> dict:
    return copy.deepcopy(schema.get("properties", {}).get(field, {}))


def _build_mapped_property(
    source_name: str,
    target_name: str,
    source_schema: dict,
    target_schema: dict,
    special_rules: dict | None = None,
) -> dict:
    src_prop = _safe_get_property(source_schema, source_name)
    tgt_prop = _safe_get_property(target_schema, target_name)

    tgt_prop.pop('x-sourcePriority', None)
    tgt_prop["x-description"] = "All enum values are harmonised to Cancer Registry coding"

    out = copy.deepcopy(tgt_prop) if tgt_prop else copy.deepcopy(src_prop)

    if isinstance(out, dict):
        out["x-sourceField"] = source_name
        out["x-harmonizedTo"] = target_name

        if src_prop.get("enum") is not None and tgt_prop.get("enum") is not None:
            out["x-enumMap"] = build_enum_mapping(
                src_prop.get("enum", []),
                tgt_prop.get("enum", []),
                special_rules=special_rules,
            )

        out = _copy_common_annotations(src_prop, out)

    return out


def update_histopath_schema_for_pipeline(
    source_schema: dict,
    target_schema: dict,
    variable_mapping: dict,
    special_rules: dict | None = None,
    include_derived_fields: bool = True,
) -> dict:
    """
    Reflect the harmonised pipeline output:
    - mapped source vars renamed to target names
    - mapped enum vars use target enums/descriptions
    - unmapped vars retained
    - derived fields added from target schema where appropriate
    """
    schema = copy.deepcopy(source_schema)
    props = schema.get("properties", {})
    out_props = OrderedDict()

    for src_name, src_prop in props.items():
        if src_name in variable_mapping:
            tgt_name = variable_mapping[src_name]
            rules = (special_rules or {}).get(src_name)
            out_props[tgt_name] = _build_mapped_property(
                source_name=src_name,
                target_name=tgt_name,
                source_schema=source_schema,
                target_schema=target_schema,
                special_rules=rules,
            )
        else:
            out_props[src_name] = copy.deepcopy(src_prop)

    if include_derived_fields:
        derived_targets = {
            "STAGE": ["Tstage", "NStage", "MStage"],
            "TUMOUR_SIZE": ["SizeInvasiveTumour", "SizeDCISOnly"],
            "NODES_TOTAL": ["AxillaryNodesTotal", "OtherNodesTotal"],
            "NODES_POSITIVE": ["AxillaryNodesPositive", "OtherNodesPositive"],
            "AGE_AT_DIAGNOSIS": [
                "Mailing.People.DOBYear",
                "Mailing.People.DOBMonth",
                "Mailing.People.DOBDay",
                "#/properties/DiagDat",
            ],
        }

        for field_name, derived_from in derived_targets.items():
            tgt_prop = target_schema.get("properties", {}).get(field_name)
            tgt_prop.pop('x-sourcePriority', None)
            tgt_prop.pop('x-description', None)
            if field_name == "STAGE":
                tgt_prop['x-description']= "All enum values are harmonised to Cancer Registry coding"
            if field_name not in out_props and tgt_prop:
                out_props[field_name] = copy.deepcopy(tgt_prop)
                out_props[field_name]["x-derivedFrom"] = copy.deepcopy(derived_from)

    schema["properties"] = out_props

    if isinstance(schema.get("required"), list):
        remapped = []
        for field in schema["required"]:
            remapped.append(variable_mapping.get(field, field))
        schema["required"] = list(OrderedDict.fromkeys(remapped))

    return schema


# -----------------------------
# Histopath pseudo-anon schema
# -----------------------------

def _filter_and_rename_properties_ordered(props: dict) -> OrderedDict:
    out = OrderedDict()
    for key, value in props.items():
        if key in DROP_FIELDS:
            continue

        new_key = "TCode" if key == "StudyID" else RENAME_MAP.get(key, key)

        if key == "StudyID":
            new_value = {
                "name": "TCode",
                "description": "Pseudoanonymised unique participant identifier.",
                "type": "string",
                "minLength": 8,
                "maxLength": 8,
                "x-formerName": "StudyID",
            }
            new_value = _copy_title_annotations(value, new_value)
        else:
            new_value = copy.deepcopy(value)
            if isinstance(new_value, dict):
                if "name" in new_value:
                    new_value["name"] = new_key
                if new_key != key:
                    _add_or_update(new_value, "x-formerName", key)
                if new_key == "DIAGNOSIS_DATE_SHIFTED":
                    _add_or_update(
                        new_value,
                        "description",
                        "Pseudo-anonymised diagnosis date shifted by the participant-level random day offset from SID codes.",
                    )
                    _add_or_update(new_value, "x-derivedFrom", [DATE_SHIFT_SOURCE_POINTER])

        out[new_key] = new_value

    # ensure TCode is first
    if "TCode" in out:
        ordered = OrderedDict()
        ordered["TCode"] = out["TCode"]
        for k, v in out.items():
            if k != "TCode":
                ordered[k] = v
        return ordered

    return out

def update_histopath_schema_for_pseudoanon(schema: dict) -> dict:
    schema = copy.deepcopy(schema)
    schema = _update_schema_metadata(schema)

    props = schema.get("properties", {})
    schema["properties"] = _filter_and_rename_properties_ordered(props)

    if "required" in schema and isinstance(schema["required"], list):
        new_required = []
        for field in schema["required"]:
            if field in DROP_FIELDS:
                continue
            if field == "StudyID":
                new_required.append("TCode")
            else:
                new_required.append(RENAME_MAP.get(field, field))
        schema["required"] = new_required

    if "x-primaryKey" in schema:
        schema["x-primaryKey"] = _remap_primary_key(schema["x-primaryKey"])

    return schema


def update_histopath_schema_for_pipeline_and_pseudoanon(
    source_schema: dict,
    target_schema: dict,
    variable_mapping: dict,
    special_rules: dict | None = None,
) -> dict:
    pipeline_schema = update_histopath_schema_for_pipeline(
        source_schema=source_schema,
        target_schema=target_schema,
        variable_mapping=variable_mapping,
        special_rules=special_rules,
        include_derived_fields=True,
    )
    return update_histopath_schema_for_pseudoanon(pipeline_schema)


def write_pipeline_pseudoanon_schema(
    in_source_schema_path: str,
    in_target_schema_path: str,
    out_schema_path: str,
    variable_mapping: dict = None,
    special_rules: dict | None = None,
) -> str:
    variable_mapping = variable_mapping or brca_variables_to_map
    special_rules = special_rules or brca_special_rules

    with open(in_source_schema_path, "r", encoding="utf-8") as f:
        source_schema = json.load(f)

    with open(in_target_schema_path, "r", encoding="utf-8") as f:
        target_schema = json.load(f)

    final_schema = update_histopath_schema_for_pipeline_and_pseudoanon(
        source_schema=source_schema,
        target_schema=target_schema,
        variable_mapping=variable_mapping,
        special_rules=special_rules,
    )

    with open(out_schema_path, "w", encoding="utf-8") as f:
        json.dump(final_schema, f, indent=2, ensure_ascii=False)

    return out_schema_path