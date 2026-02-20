import sys
import os
import pandas as pd
import copy
import json
import re
from collections import OrderedDict

sys.path.append(os.path.abspath(
    "N:\\CancerEpidem\\BrBreakthrough\\DeliveryProcess\\Schema_and_Derivation_utils\\Questionnaire\\R0\\scripts"
))
from pseudo_anon_utils import load_sid_codes, pseudo_anonymize_studyid


# -----------------------------
# INCLUSIVE allowlists (KEEP)
# -----------------------------
# Put *only* the fields you want to retain at each level.
# NOTE: include LabTracking and TMAs themselves in tumour keep-list because they are nested arrays.
KEEP_FIELDS_TUMOUR = {
    "StudyID", "LabNo", "BlockSide", "TumourCount", "ReportCount",
    "CoreBiopsy_Flag", "MegaBlock_Flag", "LymphNodes_Flag",
    "LabTracking", "TMAs"
}


KEEP_FIELDS_LAB = {
    "LabSampleType", "Scan"
}

KEEP_FIELDS_TMA = {
    "ArrayID", "ArrayNo", "ArraySister", "ArraySection",
    "XAxis", "YAxis", "TMACoreID"
}

# -----------------------------
# Provenance helpers for derived / renamed fields
# -----------------------------
# Add schema annotations similar to DerivedVariables_Schema.json:
#   - x-derivedFrom: json pointers / field names used to derive a field
#   - x-formerName: former field name when a variable was renamed
#
# Flags in this pipeline are derived from TumourTracking.BlockComments.
FLAG_FIELDS = {"CoreBiopsy_Flag", "MegaBlock_Flag", "LymphNodes_Flag"}
BLOCKCOMMENTS_POINTER = "#/properties/TumourTracking/items/properties/BlockComments"

# Variables renamed in the pseudo-anonymised output (new_name -> former_name).
# (We handle both title-case and any accidental upper-case variants to be robust.)
FORMER_NAME_MAP = {
    "TCode": "StudyID",
    "LATERALITY": "BlockSide",
    "TUMOUR_COUNT": "TumourCount",
    "REPORT_COUNT": "ReportCount"
}

def _add_or_update(d: dict, key: str, value):
    if isinstance(d, dict):
        d[key] = value

def apply_provenance_annotations_to_schema(schema: dict) -> dict:
    """
    Mutates and returns schema:
      - Adds x-derivedFrom for flag fields (derived from BlockComments)
      - Adds x-formerName for renamed variables (e.g., Laterality <- BlockSide)
    """
    if not isinstance(schema, dict):
        return schema

    # Top-level: TCode former name
    props = schema.get("properties", {})
    if "TCode" in props and isinstance(props["TCode"], dict):
        _add_or_update(props["TCode"], "x-formerName", FORMER_NAME_MAP["TCode"])

    # TumourTracking level
    try:
        tt_props = schema["properties"]["TumourTracking"]["items"]["properties"]
    except Exception:
        return schema

    # Apply former name annotations for tumour-level renamed vars
    for new_name, old_name in FORMER_NAME_MAP.items():
        if new_name in tt_props and isinstance(tt_props[new_name], dict):
            _add_or_update(tt_props[new_name], "x-formerName", old_name)

    # Apply flag provenance
    for flag in FLAG_FIELDS:
        if flag in tt_props and isinstance(tt_props[flag], dict):
            _add_or_update(tt_props[flag], "x-derivedFrom", [BLOCKCOMMENTS_POINTER])
            # Optional: a human explanation (matches derived schema style)
            if "x-description" not in tt_props[flag]:
                _add_or_update(tt_props[flag], "x-description", "Derived from TumourTracking.BlockComments using case-insensitive string/regex matching.")

    # LabTracking / TMAs: also apply former names if any exist there (rare but safe)
    for nested_key in ("LabTracking", "TMAs"):
        try:
            nested_props = tt_props[nested_key]["items"]["properties"]
        except Exception:
            continue
        for new_name, old_name in FORMER_NAME_MAP.items():
            if new_name in nested_props and isinstance(nested_props[new_name], dict):
                _add_or_update(nested_props[new_name], "x-formerName", old_name)

    return schema

# -----------------------------
# Rename output fields (post-build)
# -----------------------------
RENAME_MAP = {
    "BlockSide": "LATERALITY",
    "TumourCount": "TUMOUR_COUNT",
    "ReportCount": "REPORT_COUNT",
}

def _walk_and_rename_keys(obj, mapping: dict):
    """Recursively rename dict keys anywhere in nested dict/list."""
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


# -----------------------------
# Helpers
# -----------------------------
def _filter_dict_inplace(d: dict, keep: set[str]):
    """Remove keys not in keep, preserving only allowlisted keys."""
    if not isinstance(d, dict):
        return
    for k in list(d.keys()):
        if k not in keep:
            d.pop(k, None)


def _derive_blockcomment_flags(blockcomments):
    """Return (core_flag, mega_flag, lymph_flag) as integers 0/1.

    CoreBiopsy_Flag uses this regex (case-insensitive logic is embedded):
      ^(?!.*(?i:with\s+cores?|core.*missing|core.*rec['’]?d\s+earlier))(?=.*(?i:core)).*$

    Meaning: the text must contain the word "core", but excludes text implying the core
    is elsewhere ("with core(s)"), was received previously ("rec'd earlier" / "recd earlier"),
    or is missing.
    """
    if blockcomments is None:
        return 0, 0, 0
    s = str(blockcomments)
    low = s.lower()
    # Regex supplied by user; DOTALL makes '.*' span newlines just in case.
    core_regex = re.compile(
        r"^(?!.*(?i:with\s+cores?|core.*missing|core.*rec['’]?d\s+earlier))(?=.*(?i:core)).*$",
        flags=re.DOTALL,
    )
    core_flag = 1 if core_regex.search(s) else 0
    mega_flag = 1 if "mega" in low else 0
    lymph_terms = ["node", "nodes", "axillary clearance", "axillary sample"]
    lymph_flag = 1 if any(t in low for t in lymph_terms) else 0
    return core_flag, mega_flag, lymph_flag


def _rewrite_labid_to_tcode_labno(obj: dict, tcode: str):
    """
    If obj contains LabID, rewrite it to:
      - f"{tcode}/{LabNo}" when LabNo is present
      - else replace prefix before '/' with tcode (keeps suffix)
    """
    if not isinstance(obj, dict) or not tcode:
        return

    if "LabID" in obj:
        labno = obj.get("LabNo")
        if labno is not None:
            obj["LabID"] = f"{tcode}/{labno}"
        else:
            labid = obj.get("LabID")
            if labid is not None:
                s = str(labid)
                if "/" in s:
                    obj["LabID"] = f"{tcode}/{s.split('/', 1)[1]}"
                else:
                    obj["LabID"] = s

def _walk_and_rewrite(obj, tcode: str):
    """Recursively traverse dict/list and rewrite LabID wherever found."""
    if isinstance(obj, dict):
        _rewrite_labid_to_tcode_labno(obj, tcode)
        for v in obj.values():
            _walk_and_rewrite(v, tcode)
    elif isinstance(obj, list):
        for item in obj:
            _walk_and_rewrite(item, tcode)


# -----------------------------
# Main: apply pseudo-anon + filtering
# -----------------------------
def apply_pathology_privacy_transforms(data: list[dict], server: str, logger):
    """
    - Loads SIDCodes to map StudyID -> TCode
    - Filters each level using KEEP_* allowlists (inclusive)
    - Rewrites all LabID fields anywhere in the nested structure to TCode/LabNo
    - Replaces top-level StudyID with TCode (existing helper)
    """
    sid_df = load_sid_codes(server, logger)
    tcode_map = sid_df.set_index("StudyID")["TCode"].to_dict()

    for rec in data:
        sid = rec.get("StudyID")
        tcode = tcode_map.get(int(sid)) if sid is not None else None

        tumours = rec.get("TumourTracking") or []
        for tumour in tumours:
            # --- Derive flags from BlockComments (case-insensitive) then drop BlockComments ---
            core_f, mega_f, lymph_f = _derive_blockcomment_flags(tumour.get("BlockComments"))
            tumour["CoreBiopsy_Flag"] = core_f
            tumour["MegaBlock_Flag"] = mega_f
            tumour["LymphNodes_Flag"] = lymph_f
            tumour.pop("BlockComments", None)

            # Filter tumour-level keys (but keep LabTracking/TMAs containers)
            _filter_dict_inplace(tumour, KEEP_FIELDS_TUMOUR)

            # Filter LabTracking
            labs = tumour.get("LabTracking") or []
            for lab in labs:
                _filter_dict_inplace(lab, KEEP_FIELDS_LAB)

            # Filter TMAs
            tmas = tumour.get("TMAs") or []
            for tma in tmas:
                _filter_dict_inplace(tma, KEEP_FIELDS_TMA)

        # Rewrite LabID everywhere under this Study record
        if tcode:
            _walk_and_rewrite(rec, tcode)

    # Replace top-level StudyID -> TCode (your existing helper)
    data = pseudo_anonymize_studyid(data, sid_df)

    # Rename selected fields after pseudo-anonymisation
    _walk_and_rename_keys(data, RENAME_MAP)
    return data


# -----------------------------
# Schema update: inclusive keep-lists
# -----------------------------
def _filter_properties_ordered(props: dict, keep: set[str]) -> OrderedDict:
    """Return OrderedDict with only keys in keep, preserving original order."""
    out = OrderedDict()
    for k, v in props.items():
        if k in keep:
            out[k] = v
    return out


def _build_tumour_props_with_flags(original_tt_props: dict, keep: set[str]) -> OrderedDict:
    """
    Build tumour-level properties preserving original order, but:
      - excludes BlockComments
      - inserts CoreBiopsy_Flag, MegaBlock_Flag, LymphNodes_Flag in the position where BlockComments was
      - includes only properties in `keep` plus these inserted flags.
    """
    out = OrderedDict()
    inserted = False

    def _flag_schema(name: str, short_desc: str, xdesc: str) -> dict:
        return {
            "name": name,
            "description": short_desc,
            "type": ["integer", "null"],
            "minimum": 0,
            "maximum": 1,
            "x-derivedFrom": [BLOCKCOMMENTS_POINTER],
            "x-description": xdesc,
        }

    for k, v in original_tt_props.items():
        if k == "BlockComments" and not inserted:
            # Insert flags here (if requested in keep list)
            core_regex = r"^(?!.*(?i:with\s+cores?|core.*missing|core.*rec['’]?d\s+earlier))(?=.*(?i:core)).*$"

            out["CoreBiopsy_Flag"] = _flag_schema(
                "CoreBiopsy_Flag",
                "Flag indicating whether the block was core biopsy or not.",
                (
                    "Derived from TumourTracking.BlockComments using regex:\n"
                    f"{core_regex}\n"
                    "Rule: BlockComments must contain 'core' (case-insensitive), but excludes text implying the core is elsewhere "
                    "('with core' / 'with cores'), missing ('core...missing'), or previously received ('core...rec'd earlier' / 'core...recd earlier')."
                ),
            )

            out["MegaBlock_Flag"] = _flag_schema(
                "MegaBlock_Flag",
                "Flag indicating whether the block was mega block or not.",
                (
                    "Derived from TumourTracking.BlockComments: set to 1 if BlockComments contains substring 'mega' "
                    "(case-insensitive), else 0."
                ),
            )

            out["LymphNodes_Flag"] = _flag_schema(
                "LymphNodes_Flag",
                "Flag indicating whether the block was lymph node related or not.",
                (
                    "Derived from TumourTracking.BlockComments: set to 1 if BlockComments contains any of the following terms "
                    "(case-insensitive): 'node', 'nodes', 'axillary clearance', 'axillary sample'; else 0."
                ),
            )
            inserted = True
            continue

        if k in keep:
            out[k] = v

    # If BlockComments wasn't present, append flags at end (still deterministic)
    if not inserted:
        for fk, fd in [
            ("CoreBiopsy_Flag", "Flag derived from BlockComments: 1 if comment contains 'core' (case-insensitive), else 0."),
            ("MegaBlock_Flag", "Flag derived from BlockComments: 1 if comment contains 'mega' (case-insensitive), else 0."),
            ("LymphNodes_Flag", "Flag derived from BlockComments: 1 if comment contains any of 'node(s)', 'axillary clearance', or 'axillary sample' (case-insensitive), else 0.")
        ]:
            if fk in keep and fk not in out:
                out[fk] = {
                    "name": fk,
                    "description": fd,
                    "type": ["integer", "null"],
                    "minimum": 0,
                    "maximum": 1,
                }

    return out

def _rename_properties_ordered(props: dict, mapping: dict) -> OrderedDict:
    """Rename schema property keys preserving order; also update inner 'name' field if present."""
    out = OrderedDict()
    for k, v in props.items():
        new_k = mapping.get(k, k)
        if isinstance(v, dict) and "name" in v:
            v = copy.deepcopy(v)
            v["name"] = new_k
        out[new_k] = v
    return out

def update_schema_for_pseudoanon(schema: dict) -> dict:
    """
    Updates schema to match allowlisted output:
      - replaces StudyID with TCode at top level (same position)
      - filters TumourTracking/LabTracking/TMAs properties to KEEP_* lists (preserves order)
      - updates LabID description to reflect 'TCode/LabNo'
    """
    s = copy.deepcopy(schema)

    # Metadata
    if "$id" in s and isinstance(s["$id"], str):
        if s["$id"].endswith(".json"):
            s["$id"] = s["$id"].replace(".json", "_PseudoAnon.json")
        else:
            s["$id"] = s["$id"] + "_PseudoAnon"
    if "title" in s:
        s["title"] = f"{s['title']}_PseudoAnon"

    # Replace StudyID -> TCode in properties order
    original_props = s["properties"]
    new_props = OrderedDict()
    for key, value in original_props.items():
        if key == "StudyID":
            new_props["TCode"] = {
                "name": "TCode",
                "description": "Pseudoanonymised participant identifier (TCode) replacing StudyID. Derived from SIDCodes mapping.",
                "type": ["string", "null"],
            }
        else:
            new_props[key] = value
    s["properties"] = new_props

    # required order
    if "required" in s:
        s["required"] = ["TCode" if x == "StudyID" else x for x in s["required"]]
    else:
        s["required"] = ["TCode"]

    # TumourTracking properties
    tt_props = s["properties"]["TumourTracking"]["items"]["properties"]

    # Filter tumour properties to allowlist (order preserved)
    tt_props_filtered = _build_tumour_props_with_flags(tt_props, KEEP_FIELDS_TUMOUR)
    s["properties"]["TumourTracking"]["items"]["properties"] = tt_props_filtered
    # Rename tumour-level fields in schema to match pseudo-anon output (preserve order)
    tt_props_renamed = _rename_properties_ordered(tt_props_filtered, RENAME_MAP)
    s["properties"]["TumourTracking"]["items"]["properties"] = tt_props_renamed
    tt_props_filtered = tt_props_renamed


    # Update LabID description if present in tumour
    if "LabID" in tt_props_filtered:
        tt_props_filtered["LabID"]["description"] = (
            "Pseudoanonymised identifier combining TCode and LabNo in the format 'TCode/LabNo'."
        )

    # LabTracking properties
    lab_props = tt_props_filtered["LabTracking"]["items"]["properties"]
    lab_props_filtered = _filter_properties_ordered(lab_props, KEEP_FIELDS_LAB)
    tt_props_filtered["LabTracking"]["items"]["properties"] = lab_props_filtered
    # Rename any renamed fields in LabTracking schema as well (safe no-op if not present)
    lab_props_renamed = _rename_properties_ordered(lab_props_filtered, RENAME_MAP)
    tt_props_filtered["LabTracking"]["items"]["properties"] = lab_props_renamed
    lab_props_filtered = lab_props_renamed


    if "LabID" in lab_props_filtered:
        lab_props_filtered["LabID"]["description"] = (
            "Pseudoanonymised identifier combining TCode and LabNo in the format 'TCode/LabNo'."
        )

    # TMAs properties
    tma_props = tt_props_filtered["TMAs"]["items"]["properties"]
    tma_props_filtered = _filter_properties_ordered(tma_props, KEEP_FIELDS_TMA)
    tt_props_filtered["TMAs"]["items"]["properties"] = tma_props_filtered
    # Rename any renamed fields in TMAs schema as well (safe no-op if not present)
    tma_props_renamed = _rename_properties_ordered(tma_props_filtered, RENAME_MAP)
    tt_props_filtered["TMAs"]["items"]["properties"] = tma_props_renamed
    tma_props_filtered = tma_props_renamed


    if "LabID" in tma_props_filtered:
        tma_props_filtered["LabID"]["description"] = (
            "Pseudoanonymised identifier combining TCode and LabNo in the format 'TCode/LabNo'."
        )
    # Add provenance annotations (x-derivedFrom / x-formerName)
    s = apply_provenance_annotations_to_schema(s)


    return s


def write_pseudoanon_schema(in_schema_path: str, out_schema_path: str):
    with open(in_schema_path, "r") as f:
        schema = json.load(f)

    new_schema = update_schema_for_pseudoanon(schema)

    with open(out_schema_path, "w") as f:
        json.dump(new_schema, f, indent=2, ensure_ascii=False)

    return out_schema_path
