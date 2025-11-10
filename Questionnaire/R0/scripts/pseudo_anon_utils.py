"""
Pseudo-anonymisation utilities for questionnaire data.

This module handles:
- Loading StudyID <-> TCode mappings from the Mailing database.
- Building lookup structures for navigating JSON Schemas by field name.
- Deriving date variables from day/month/year components and removing
  the original components from the output.
- Replacing StudyID with TCode and optionally shifting dates to protect
  participant privacy.
- Updating section schemas to describe the pseudo-anonymised output.

It is used near the end of each ETL pipeline, just before validation
and final JSON export.
"""

import sys
import os
import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from cleaning_utils import convert_to_date
import json
from collections import OrderedDict
from nested_utils import rename_variable

sys.path.append(os.path.abspath("N:\\CancerEpidem\\BrBreakthrough\\DeliveryProcess\\Schema_and_Derivation_utils"))
from utilities import connect_DB, read_data

def load_sid_codes(server, logger):
    """
    Load StudyID <-> TCode mappings from the SIDCodes table.

    Parameters
    ----------
    server : str
        SQL Server name or connection string fragment.
    logger :
        Optional logger for progress messages.

    Returns
    -------
    pd.DataFrame
        DataFrame with at least StudyID and TCode columns.
    """
    dm_conn_sid = connect_DB('Mailing', server, logger)
    sid_df = read_data("SELECT * FROM [Mailing].[dbo].[SIDCodes]", dm_conn_sid, logger)
    sid_df['StudyID'] = sid_df['StudyID'].astype(int)
    return sid_df

def build_name_to_fieldkey(schema_node, include_nested=False, prefix=''):
    """
    Build a mapping from JSON Schema 'name' attributes to JSON pointer keys.

    This allows us to locate fields in the nested data structure using the
    original variable names (e.g. R0_XrayDay) rather than their full path.
    """
    name_to_key = {}
    properties = schema_node.get("properties", {})
    for key, val in properties.items():
        full_key = f"{prefix}{key}" if prefix else key

        # Map the JSON key itself
        name_to_key[key] = full_key

        if isinstance(val, dict):
            # Also map the "name" (SQL/Questionnaire variable) if present
            if "name" in val:
                name_to_key[val["name"]] = full_key

            if include_nested:
                if "properties" in val:
                    nested_map = build_name_to_fieldkey(val, True, f"{full_key}.")
                    name_to_key.update(nested_map)
                elif val.get("type") == "array" and "items" in val:
                    nested_map = build_name_to_fieldkey(val["items"], True, f"{full_key}[].")
                    name_to_key.update(nested_map)
    return name_to_key

def build_path_keys(flat_date_dict, name_to_key):
    """
    Build a mapping from schema field keys to their JSON pointer paths.

    The resulting dict makes it easy to navigate deeply nested structures
    when deriving dates or removing fields.
    """
    path_keys_dict = {}
    for new_field, var_names in flat_date_dict.items():
        path_keys = []
        for name in var_names:
            if name in name_to_key:
                base_path = name_to_key[name]
                if "[]" in base_path:
                    path = base_path.replace("[]", "[*]")
                    path_keys.append(path)
                else:
                    path_keys.append(base_path)
        if path_keys:
            path_keys_dict[new_field] = path_keys
    return path_keys_dict

def _common_container_prefix(paths):
    """
    Given a list of normalized JSON paths (with [*] for arrays), return the list of
    path segments that form the common container prefix (excluding the leaf keys).
    Example:
      ["XrayEvents[*].XrayEventsExtra[*].R0_XrayMonth",
       "XrayEvents[*].XrayEventsExtra[*].R0_XrayYear_Extra"]
    -> ["XrayEvents[*]", "XrayEventsExtra[*]"]
    """
    seg_lists = [p.split('.')[:-1] for p in paths]  # drop the leaf
    if not seg_lists:
        return []
    prefix = []
    for cols in zip(*seg_lists):
        if all(s == cols[0] for s in cols):
            prefix.append(cols[0])
        else:
            break
    return prefix

def extract_from_record(record, path):
    parts = path.split('.')
    current = record
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current

def set_in_record(record, path, value):
    parts = path.split('.')
    for part in parts[:-1]:
        if part not in record or not isinstance(record[part], dict):
            record[part] = {}
        record = record[part]
    record[parts[-1]] = value

def extract_date_from_parts(parts):
    """
    Accepts components in this order:
      • [day, month, year]
      • [month, year]
      • [year]
    Returns a datetime or None. Two-digit years are folded to 19xx/20xx.
    """
    # If all components are missing/empty -> None
    if not parts or all(p is None or str(p).strip() == "" for p in parts):
        return None

    try:
        # Keep only non-empty values, cast to ints
        cleaned = []
        for p in parts:
            if p is None:
                continue
            s = str(p).strip()
            if s == "":
                continue
            cleaned.append(int(s))

        # Map by length (the input order must match dateDict component order)
        if len(cleaned) == 3:
            day, month, year = cleaned
        elif len(cleaned) == 2:
            month, year = cleaned
            day = 15              # mid-month when only month+year are given
        elif len(cleaned) == 1:
            year = cleaned[0]
            month, day = 7, 1     # mid-year when only year is given
        else:
            return None

        # 2-digit year handling (match your existing convention)
        if year < 100:
            year += 1900 if year > 25 else 2000

        return datetime(year, month, day)
    except Exception:
        return None


def drop_original_fields(data_list, flat_date_dict):
    leaf_names = set()
    for components in flat_date_dict.values():
        leaf_names.update(components)
    fields_to_remove = leaf_names | {"Random"}

    def remove_nested_fields(node):
        if isinstance(node, dict):
            for key in list(node.keys()):
                if key in fields_to_remove:
                    del node[key]
                else:
                    remove_nested_fields(node[key])
        elif isinstance(node, list):
            for item in node:
                remove_nested_fields(item)

    for record in data_list:
        remove_nested_fields(record)
    return data_list

  
def process_dates(data, sid_df, schema, logger, dateDict):
    """
    Derive full date fields from day/month/year components and remove the parts.

    For each section:
    - Use `dateDict` to identify which components belong to which derived date.
    - Locate the corresponding fields in the nested JSON using schema lookups.
    - Assemble a full date (where possible) and store it in the derived field.
    - Remove the original component fields from the records.

    Parameters
    ----------
    data_list : list[dict]
        Pseudo-anonymised JSON-like records.
    sid_df : pd.DataFrame
        StudyID/TCode mappings (kept for consistency, even if not directly used).
    schema : dict
        JSON Schema for the section, used to resolve paths and types.
    logger :
        Logger for progress messages.
    dateDict : dict
        Per-section config describing date components and question ranges.
    """

    def _detect_role(var_name: str) -> str | None:
        """
        Infer 'day' | 'month' | 'year' by variable name/key.
        Supports BOTH snake_case and CamelCase suffixes:
        ..._Day / ...Day, ..._(Month|Mnth|Mth) / ...Month|Mnth|Mth, ..._(Year|Yr) / ...Year|Yr
        Also supports DOBd/m/y and *_D/_M/_Y.
        """
        s = str(var_name)

        # High-confidence suffixes (CamelCase or underscore)
        # Day
        if re.search(r'(?:^|_)(Day|D)$', s, flags=re.I) or re.search(r'(Day|D)$', s, flags=re.I):
            return 'day'
        # Month
        if re.search(r'(?:^|_)(Month|Mnth|Mth|M)$', s, flags=re.I) or re.search(r'(Month|Mnth|Mth|M)$', s, flags=re.I):
            return 'month'
        # Year
        if re.search(r'(?:^|_)(Year|Yr|Y)$', s, flags=re.I) or re.search(r'(Year|Yr|Y)$', s, flags=re.I):
            return 'year'

        # DOB short tokens used in your sheets
        if re.search(r'(?:^|_)DOBd$', s, flags=re.I): return 'day'
        if re.search(r'(?:^|_)DOBm$', s, flags=re.I): return 'month'
        if re.search(r'(?:^|_)DOBy$', s, flags=re.I): return 'year'

        # Very broad fallbacks (last resort)
        if 'month' in s.lower() or 'mnth' in s.lower() or 'mth' in s.lower():
            return 'month'
        if 'year'  in s.lower() or 'yr'   in s.lower():
            return 'year'
        if 'day'   in s.lower():
            return 'day'

        return None

    def _assemble_date_by_granularity(parts: dict, expected_roles: set):
        """
        parts: {'day': v|None, 'month': v|None, 'year': v|None}
        expected_roles: set like {'year'} or {'month','year'} or {'day','month','year'}

        Rules:
          • If 'year' is expected but missing → None (even if day/month present).
          • Day+Year with no Month → treat as YEAR-only (ignore day).
          • Degrade gracefully: DMY > MY > Y (never invent a year).
        """
        d, m, y = parts.get('day'), parts.get('month'), parts.get('year')

        def _as_int(x):
            if x is None: return None
            try: return int(str(x).strip())
            except Exception: return None

        d, m, y = _as_int(d), _as_int(m), _as_int(y)

        # Year expected but missing → invalid
        if 'year' in expected_roles and y is None:
            return None

        # Two-digit year handling (consistent with your existing code paths)
        if y is not None and y < 100:
            y = 1900 + y if y > 25 else 2000 + y

        # Day+Year without Month → degrade to Year-only
        if (y is not None) and (m is None) and (d is not None):
            d = None

        try:
            if y is not None and m is not None and d is not None:
                return datetime(y, m, d)
            if y is not None and m is not None:
                return datetime(y, m, 15)  # mid-month
            if y is not None:
                return datetime(y, 7, 1)   # mid-year
        except Exception:
            return None
        return None

    # Flatten all section dateDicts
    flat_date_dict = {}
    for section in dateDict.values():
        flat_date_dict.update(section.get("dateDict", {}))

    # Keep the original component names per derived field (for role detection)
    flat_names = {new_field: comps for new_field, comps in flat_date_dict.items()}

    # Map variable names to JSON paths
    name_to_key = build_name_to_fieldkey(schema, include_nested=True) 

    def _normalize_to_star(path: str) -> str:
        # Convert '[]' to '[*]' for wildcard iteration
        return path.replace('[]', '[*]')

    def _build_path_keys(flat_date_dict, name_to_key):
        out = {}
        for new_field, var_names in flat_date_dict.items():
            paths = []
            for name in var_names:
                if name in name_to_key:
                    paths.append(_normalize_to_star(name_to_key[name]))
            if paths:
                out[new_field] = paths
        return out

    path_keys_dict = _build_path_keys(flat_date_dict, name_to_key)
    
    # Attach per-StudyID day-shift
    random_map = sid_df.set_index('StudyID')['Random'].astype(int).to_dict()
    for record in data:
        record['R0_StudyID'] = int(record['R0_StudyID'])
        record['Random'] = random_map.get(record['R0_StudyID'], 0)

    # Utilities for navigating and writing
    def _common_container_prefix(paths):
        seg_lists = [p.split('.')[:-1] for p in paths]
        if not seg_lists: return []
        pref = []
        for cols in zip(*seg_lists):
            if all(s == cols[0] for s in cols):
                pref.append(cols[0])
            else:
                break
        return pref

    def _get_rel(obj, rel_path):
        cur = obj
        for part in rel_path.split('.'):
            if isinstance(cur, dict):
                cur = cur.get(part)
            else:
                return None
        return cur

    def _set_in_record(record, path, value):
        parts = path.split('.')
        for part in parts[:-1]:
            if part not in record or not isinstance(record[part], dict):
                record[part] = {}
            record = record[part]
        record[parts[-1]] = value

    def _set_rel(obj, key, value):
        obj[key] = value

    # Main derivation loop
    for record in data:
        shift_days = int(record.get('Random', 0))

        for new_field, comp_paths in path_keys_dict.items():
            if not comp_paths:
                continue

            container_segs = _common_container_prefix(comp_paths)
            stars = sum(seg.count('[*]') for seg in container_segs)

            rel_leafs = [p.split('[*].')[-1] for p in comp_paths]

            # Expected roles for this derived field from component names
            expected_roles = set()
            for comp_name in flat_names.get(new_field, []):
                role = _detect_role(comp_name) or 'year'  # conservative default
                expected_roles.add(role)

            def _assemble_for_obj(obj):
                # Build {'day':..., 'month':..., 'year':...} by reading values from obj
                parts = {'day': None, 'month': None, 'year': None}
                for comp_name, leaf in zip(flat_names.get(new_field, []), rel_leafs):
                    role = _detect_role(comp_name) or _detect_role(leaf)
                    if role:
                        parts[role] = _get_rel(obj, leaf)
                if new_field == "R0_XRayDate":
                    xnum = obj.get("R0_XrayNum")
                    if xnum is not None:
                        try:
                            xnum = int(xnum)
                            if 1 <= xnum <= 3:   # Option A: year only
                                parts['month'] = None
                            elif 4 <= xnum <= 12:  # Option B: month+year
                                pass
                        except Exception:
                            pass
                        
                return _assemble_date_by_granularity(parts, expected_roles)

            if stars == 0:
                # Top-level container
                dt = _assemble_for_obj(record)
                if isinstance(dt, datetime):
                    dt = dt + timedelta(days=shift_days)
                    _set_in_record(record, new_field, dt.strftime("%Y-%m-%d"))
                else:
                    _set_in_record(record, new_field, None)

            elif stars == 1:
                # One-level array, e.g., Parent[*]
                top_key = container_segs[0].replace('[*]', '')
                arr = record.get(top_key, [])
                if isinstance(arr, list):
                    for item in arr:
                        dt = _assemble_for_obj(item)
                        if isinstance(dt, datetime):
                            dt = dt + timedelta(days=shift_days)
                            _set_rel(item, new_field, dt.strftime("%Y-%m-%d"))
                        else:
                            _set_rel(item, new_field, None)

            elif stars == 2:
                # Two-level array, e.g., Parent[*].Child[*]
                top_key = container_segs[0].replace('[*]', '')
                sub_key = container_segs[1].replace('[*]', '')
                top_arr = record.get(top_key, [])
                if isinstance(top_arr, list):
                    for base_item in top_arr:
                        sub_arr = base_item.get(sub_key, [])
                        if isinstance(sub_arr, list):
                            for sub_item in sub_arr:
                                dt = _assemble_for_obj(sub_item)
                                if isinstance(dt, datetime):
                                    dt = dt + timedelta(days=shift_days)
                                    _set_rel(sub_item, new_field, dt.strftime("%Y-%m-%d"))
                                else:
                                    _set_rel(sub_item, new_field, None)
            else:
                # Deeper nesting not currently required; skip safely.
                continue

    # Drop original date components + Random
    return drop_original_fields(data, flat_date_dict)


def pseudo_anonymize_studyid(data, sid_df):
    """
    Replace R0_StudyID with R0_TCode in the data records.

    Uses the SIDCodes mapping to ensure there is a unique TCode per StudyID.
    """
    mapping = sid_df.set_index('StudyID')['TCode'].to_dict()
    new_data = []
    for record in data:
        new_record = {'R0_TCode': mapping.get(record.get('R0_StudyID'))}
        for key, value in record.items():
            if key not in ('R0_StudyID', 'R0_TCode'):
                new_record[key] = value
        new_data.append(new_record)
    return new_data

def apply_full_pseudo_anonymization(data, server, logger, schema=None, dateDict=None):
    sid_df = load_sid_codes(server, logger)
    if schema and dateDict:
        for record in data:
            record['R0_StudyID'] = int(record['R0_StudyID'])
        data = process_dates(data, sid_df, schema, logger, dateDict)
    data = pseudo_anonymize_studyid(data, sid_df)
    
    def convert_nans_to_none(obj):
        if isinstance(obj, dict):
            return {k: convert_nans_to_none(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_nans_to_none(i) for i in obj]
        elif isinstance(obj, float) and pd.isna(obj):
            return None
        else:
            return obj

    return convert_nans_to_none(data)

def update_schema(old_schema_path, new_schema_path, dateDict, pii_vars, section_name):
    """
    Produce a pseudo-anonymised schema from a raw section schema.

    This function:
    - Inserts new derived date fields with format "date".
    - Removes PII fields and date components that will not appear in the
      pseudo-anonymised JSON.
    - Replaces R0_StudyID with R0_TCode in definitions and required lists.
    - Adds a back-reference to the raw schema under `$defs`.

    The resulting schema describes the final, privacy-protected output.
    """

    # small helpers
    def ordered_load(path):
        with open(path, "r") as f:
            return json.load(f, object_pairs_hook=OrderedDict)

    def ordered_dump(obj, path):
        with open(path, "w") as f:
            json.dump(obj, f, indent=2)

    def normalize_path(p):
        return p.replace("[*]", "[]")

    def get_props(node):
        if isinstance(node, dict) and "properties" in node:
            return node["properties"]
        if isinstance(node, dict) and "items" in node and "properties" in node["items"]:
            return node["items"]["properties"]
        return None

    def walk_to_node(schema_obj, path_segments):
        node = schema_obj
        for seg in path_segments:
            props = get_props(node)
            if seg.endswith("[]"):
                key = seg[:-2]
                if props is None or key not in props:
                    return None
                node = props[key].get("items", {})
            else:
                if props is None or seg not in props:
                    return None
                node = props[seg]
        return node

    def resolve_parent_properties(schema_obj, full_key_path):
        parts = normalize_path(full_key_path).split(".")
        container_parts = parts[:-1]
        if not container_parts:
            return schema_obj.get("properties", OrderedDict())
        container_node = walk_to_node(schema_obj, container_parts)
        return get_props(container_node)

    def parent_container_path(full_key_path):
        parts = normalize_path(full_key_path).split(".")
        return ".".join(parts[:-1])

    def write_back_parent(schema_obj, parent_path, new_parent_props):
        if parent_path == "":
            schema_obj["properties"] = new_parent_props
            return
        parts = parent_path.split(".") if parent_path else []
        node = walk_to_node(schema_obj, parts)
        if node is None:
            return
        if "properties" in node:
            node["properties"] = new_parent_props
        elif "items" in node and "properties" in node["items"]:
            node["items"]["properties"] = new_parent_props

    def insert_replacing_components(parent_props, new_key, new_val, component_keys):
        new_props = OrderedDict()
        inserted = False
        for k, v in parent_props.items():
            if k in component_keys:
                if not inserted:
                    new_props[new_key] = new_val
                    inserted = True
                continue  # skip component(s)
            new_props[k] = v
        if not inserted:
            new_props[new_key] = new_val
        return new_props

    # Deep sweep that removes any property whose key or "name" matches our removal set,
    # and also any "family variants" (key or name that starts with base + '_' boundary).
    def deep_remove(schema_obj, drop_keys_exact, drop_names_exact, family_bases, preserve_keys):
        def is_family(name, bases):
            for b in bases:
                # exact or base_*
                if name == b or name.startswith(b + "_"):
                    return True
            return False

        def scrub_properties(props: OrderedDict):
            kill = []
            for k, v in list(props.items()):
                if not isinstance(v, dict):
                    continue
                v_name = v.get("name")
                # do not delete protected (derived) keys
                if k in preserve_keys:
                    pass
                elif (k in drop_keys_exact) or (v_name in drop_names_exact) or is_family(k, family_bases) or is_family((v_name or ""), family_bases):
                    kill.append(k)
                else:
                    # recurse into children
                    if "properties" in v and isinstance(v["properties"], dict):
                        scrub_properties(v["properties"])
                    if "items" in v and isinstance(v["items"], dict):
                        if "properties" in v["items"] and isinstance(v["items"]["properties"], dict):
                            scrub_properties(v["items"]["properties"])
            for k in kill:
                props.pop(k, None)

        props0 = schema_obj.get("properties", OrderedDict())
        scrub_properties(props0)

    # Load
    schema = ordered_load(old_schema_path)

    # Inputs
    if section_name not in dateDict:
        raise ValueError(f"Section '{section_name}' not found in dateDict")
    inner_dict = (dateDict[section_name] or {}).get("dateDict", {}) or {}

    # Provided by your utils; must include nested paths like 'XrayEvents[].R0_XrayFromYr'
    name_to_key = build_name_to_fieldkey(schema, include_nested=True)

    # Insert derived fields IN PLACE
    for new_field, components in inner_dict.items():
        if not components:
            continue

        # use first component as anchor
        first_comp = components[0]
        if first_comp in name_to_key:
            comp_full_path = name_to_key[first_comp]
            parent_props = resolve_parent_properties(schema, comp_full_path)
            parent_path = parent_container_path(comp_full_path)
        else:
            parent_props = schema.get("properties", OrderedDict())
            parent_path = ""

        # derived field schema
        derived_field = OrderedDict([
            ("name", new_field),
            ("description", "Derived date field - either a valid date or null for unknown"),
            ("type", ["string", "null"]),
            ("format", "date"),
            ("x-derivedFrom", components),
        ])

        # component keys at this level
        component_json_keys = []
        for c in components:
            if c in name_to_key:
                component_json_keys.append(
                    normalize_path(name_to_key[c]).split(".")[-1].replace("[]", "")
                )

        updated = insert_replacing_components(parent_props, new_field, derived_field, set(component_json_keys))
        write_back_parent(schema, parent_path, updated)

    # Remove PII + date component leaves (everywhere)
    # Build sets to remove; keep derived names protected
    derived_names = set(inner_dict.keys())

    # Gather components for removal (except components that equal a derived name)
    component_vars_to_remove = set()
    for comps in inner_dict.values():
        for c in comps:
            if c not in derived_names:
                component_vars_to_remove.add(c)

    # Expand raw PII vars into schema field names via rename_variable, and collect bases for family removal
    drop_keys_exact = set()   # JSON keys to drop (exact)
    drop_names_exact = set()  # "name" strings to drop (exact)
    family_bases = set()      # base names to drop with suffixes (_Extra etc.)

    def add_family_base(s: str):
        if s:
            family_bases.add(s)

    # 1) Raw PII list: try direct path mapping (covers top-level where name == Q9_…)
    for var in pii_vars:
        if var in name_to_key:
            drop_names_exact.add(var)  # name match
            # store its JSON key base (last segment) for family deletion too
            base_key = normalize_path(name_to_key[var]).split(".")[-1].replace("[]", "")
            add_family_base(base_key)

        # 2) Use rename_variable to translate raw → schema field (handles nested arrays)
        meta = rename_variable(var)
        if meta and meta.get("schema_field"):
            sf = meta["schema_field"]  # e.g., R0_XrayHospital_Extra
            if sf in name_to_key:
                # drop by JSON path
                drop_keys_exact.add(sf)
                add_family_base(sf)

        # 3) If the raw already looks like a schema key (R0_…),
        # try mapping it as a key/name
        if var in name_to_key:
            drop_keys_exact.add(normalize_path(name_to_key[var]).split(".")[-1].replace("[]", ""))
            add_family_base(var)

    # Also drop any explicit component variables by name (their paths are mapped below)
    for c in component_vars_to_remove:
        if c in name_to_key:
            drop_names_exact.add(c)
            add_family_base(c)

    # Convert the “exact” name/key sets above into concrete JSON leaf paths and remove by path
    # (This removes the leaf once per location; the deep sweep below catches any remaining.)
    refreshed = build_name_to_fieldkey(schema, include_nested=True)  # after inserts
    # Build concrete path set
    path_targets = set()
    for token in list(drop_names_exact) + list(drop_keys_exact):
        if token in refreshed:
            path_targets.add(refreshed[token])

    def remove_field_by_path(schema_obj, full_path):
        parts = normalize_path(full_path).split(".")
        if not parts:
            return
        container_parts, leaf = parts[:-1], parts[-1]
        if not container_parts:
            if "properties" in schema_obj:
                schema_obj["properties"].pop(leaf, None)
            return
        container_node = walk_to_node(schema_obj, container_parts)
        if container_node is None:
            return
        props = get_props(container_node)
        if props is None:
            return
        props.pop(leaf, None)

    for p in list(path_targets):
        remove_field_by_path(schema, p)

    # Deep sweep (by key OR "name", with family variants), preserving derived keys
    deep_remove(schema,
                drop_keys_exact=drop_keys_exact,
                drop_names_exact=drop_names_exact,
                family_bases=family_bases,
                preserve_keys=derived_names)

    # Replace StudyID with R0_TCode at top
    tcode_property = OrderedDict([
        ("name", "TCode"),
        ("description", "Pseudo-anonymized 8-character study identifier."),
        ("type", ["string"]),
        ("minLength", 8),
        ("maxLength", 8),
    ])

    top_props = schema.get("properties", OrderedDict())
    new_top = OrderedDict()
    new_top["R0_TCode"] = tcode_property
    for key, prop in top_props.items():
        if not ("name" in prop and prop["name"] == "StudyID"):
            new_top[key] = prop
    schema["properties"] = new_top

    req = schema.get("required", [])
    if "R0_StudyID" in req:
        req = [r for r in req if r != "R0_StudyID"]
    if "R0_TCode" not in req:
        req.append("R0_TCode")
    schema["required"] = req

    # Add $defs back-reference
    if "$defs" not in schema:
        schema["$defs"] = OrderedDict()
    schema["$defs"][section_name] = {"$ref": f"../raw/{section_name}_JSON.json"}

    # Write
    ordered_dump(schema, new_schema_path)
    print(f"Updated {section_name} schema written to {new_schema_path}")


# Date dictionary that shows provenance for raw date variables and is used by pseudo-anon process
dateDict = dateDict_new = {
    "GeneralInformation": {
        "dateDict": {},
        "question_range": "BETWEEN 1 AND 40"
    },
    "BirthDetails": {
        "dateDict": {
            
        },
        "question_range": "BETWEEN 51 AND 91"
    },
    "PhysicalDevelopment": {
        "dateDict": {
            "R0_RecordedHeight": ["R0_RecHght_Day", "R0_RecHght_Mnth", "R0_RecHght_Yr"]
        },
        "question_range": "BETWEEN 101 AND 187"
    },
    "Pregnancies": {
        "dateDict": {
            "R0_PregnancyEndDate": ["R0_Preg_EndDay", "R0_Preg_EndMnth", "R0_Preg_EndYr"]
        },
        "question_range": "BETWEEN 550 AND 739"
    },
    "MenstrualMenopause": {
        "dateDict": {
            "R0_TemporaryPeriodStop_Start": ["R0_TempStopFromMth", "R0_TempStopFromYr"],
            "R0_TemporaryPeriodStop_End":   ["R0_TempStopToMth",   "R0_TempStopToYr"],
            "R0_OvaryOperation":            ["R0_OvaryOp_Mnth",      "R0_OvaryOp_Yr"],
            "R0_OvaryOperation_RangeStart": ["R0_OvaryOp_StartMnth", "R0_OvaryOp_StartYr"],
            "R0_OvaryOperation_RangeEnd":   ["R0_OvaryOp_EndMnth",   "R0_OvaryOp_EndYr"]
        },
        "question_range": "BETWEEN 400 AND 544"
    },
    "Mammograms": {
        "dateDict": {
            "R0_Mammogram_Year": ["R0_Mammogram_Yr"]
        },
        "question_range": "BETWEEN 1101 AND 1110"
    },
    "AlcoholSmokingDiet": {
        "dateDict": {},
        "question_range": "BETWEEN 2150 AND 2260"
    },
    "XRays": {
        "dateDict": {
            "R0_XRayDate": ["R0_XrayMonth", "R0_XrayYear"],
            "R0_XRay_RangeStart": ["R0_XrayFromMth", "R0_XrayFromYr"],
            "R0_XRay_RangeEnd": ["R0_XrayToMth", "R0_XrayToYr"]
        },
        "question_range": "BETWEEN 1800 AND 1951"
    },
    "BreastDisease": {
        "dateDict": {
            "R0_BBD_Date": ["R0_BBD_Month", "R0_BBD_Year"],
            "R0_BBD_RangeStart": ["R0_BBD_FromMnth", "R0_BBD_FromYr"],
            "R0_BBD_RangeEnd": ["R0_BBD_ToMnth", "R0_BBD_ToYr"]
        },
        "question_range": "BETWEEN 1111 AND 1185 OR QuestionID BETWEEN 1366 AND 1370"
    },
    "BreastCancer": {
        "dateDict": {
            "R0_BreastCancerDiagnosis": ["R0_CancerDiagnosisMonth", "R0_CancerDiagnosisYear"],
            "R0_Radiotherapy_Start": ["R0_RadiotherapyStartMonth", "R0_RadiotherapyStartYear"],
            "R0_Radiotherapy_End":   ["R0_RadiotherapyEndMonth",   "R0_RadiotherapyEndYear"],
            "R0_BCDrugRegimen_Start": ["R0_BCDrugRegimenStartMonth", "R0_BCDrugRegimenStartYear"],
            "R0_BCDrugRegimen_End":   ["R0_BCDrugRegimenStopMonth",  "R0_BCDrugRegimenStopYear"]
        },
        "question_range": "BETWEEN 1186 AND 1250"
    },
    "Jobs": {
        "dateDict": {
            "R0_RadJobStartYear": ["R0_RadJobStartYr1"],
            "R0_RadJobEndYear": ["R0_RadJobEndYr1"],
            "R0_NightWorkStart": ["R0_NightWorkStartYr"],
            "R0_NightWorkEnd": ["R0_NightWorkEndYr"]
        }
    },
    "PhysicalActivity": {
        "dateDict": {},
        "question_range": "BETWEEN 2300 AND 2352"
    },
    "ContraceptiveHRT": {
        "dateDict": {
            "R0_ContracepPill_Start": ["R0_ContracepPill_StartMnth", "R0_ContracepPill_StartYr"],
            "R0_ContracepPill_End":   ["R0_ContracepPill_StopMnth",  "R0_ContracepPill_StopYr"],
            "R0_OtherContracep_Start": ["R0_OtherContracep_StartMnth", "R0_OtherContracep_StartYr"],
            "R0_OtherContracep_End":   ["R0_OtherContracep_StopMnth",  "R0_OtherContracep_StopYr"],
            "R0_HRT_Start": ["R0_HRT_StartMnth", "R0_HRT_StartYr"],
            "R0_HRT_End":   ["R0_HRT_StopMnth", "R0_HRT_StopYr"],
            "R0_OtherSexHormones_Start": ["R0_OtherSexHrmn_StartMnth", "R0_OtherSexHrmn_StartYr"],
            "R0_OtherSexHormones_End": ["R0_OtherSexHrmn_StopMnth", "R0_OtherSexHrmn_StopYr"]
        },
        "question_range": "BETWEEN 801 AND 1015"
    },
    "CancerRelatives": {
        "dateDict": {
            "R0_FatherDOB": ["R0_FatherDOB_Day", "R0_FatherDOB_Month", "R0_FatherDOB_Year"],
            "R0_FatherCancerDate": ["R0_FatherCancerYear"],
            "R0_FatherDeathDate": ["R0_FatherDOD_Day", "R0_FatherDOD_Month", "R0_FatherDOD_Year"],
            "R0_MotherDOB": ["R0_MotherDOB_Day", "R0_MotherDOB_Month", "R0_MotherDOB_Year"],
            "R0_MotherCancerDate": ["R0_MotherCancerYear"],
            "R0_MotherDeathDate": ["R0_MotherDOD_Day", "R0_MotherDOD_Month", "R0_MotherDOD_Year"],
            "R0_SiblingDOB": ["R0_Sibling_DOB_Day", "R0_Sibling_DOB_Month", "R0_Sibling_DOB_Year"],
            "R0_SiblingCancerYear": ["R0_Sibling_CancerYear"],
            "R0_ChildCancerDOB": ["R0_Child_DOB_Day", "R0_Child_DOB_Month", "R0_Child_DOB_Year"],
            "R0_ChildCancerDate": ["R0_Child_CancerYear"]
        },
        "question_range": "BETWEEN 2500 AND 2734"
    },
    "MH_Illnesses": {
        "dateDict": {
            "R0_HipFractureYear": ["R0_BrokenHip_Yr"],
            "R0_EatingDisorderDoctor_Start": ["R0_ED_DrStartMth", "R0_ED_DrStartYr"],
            "R0_EatingDisorder_2": ["R0_ED_StartMth", "R0_ED_StartYr"],
            "R0_EatingDisorder_2_RangeStart": ["R0_ED_FromMth", "R0_ED_FromYr"],
            "R0_EatingDisorder_2_RangeEnd": ["R0_ED_ToMth", "R0_ED_ToMthYr"]
        },
        "question_range": "BETWEEN 1426 AND 1546"
    },
    "MH_CancersBenignTumors": {
        "dateDict": {
            "R0_OtherCancerDiagnosisYear": ["R0_OtherCancerDiagnosisYr"]
        },
        "question_range": "BETWEEN 1400 AND 1416"
    },
    "MH_DrugsSupplements": {
        "dateDict": {
            "R0_Aspirin_Start": ["R0_AspririnStartMthEp", "R0_AspirinStartYrEp"],
            "R0_Aspirin_End": ["R0_AspririnEndMthEp", "R0_AspirinEndYrEp"],
            "R0_Ibuprofen_Start": ["R0_IbuprofenStartMthEp", "R0_IbuprofenStartYrEp"],
            "R0_Ibuprofen_End": ["R0_IbuprofenEndMthEp", "R0_IbuprofenEndYrEp"],
            "R0_OtherPainkillers_Start": ["R0_OtherPainkillersStartMth", "R0_OtherPainkillersStartYr"],
            "R0_OtherPainkillers_End": ["R0_OtherPainkillersEndMth", "R0_OtherPainkillersEndYr"]
        },
        "question_range": "BETWEEN 1600 AND 1742"
    },
    "OtherBreastSurgery": {
        "dateDict": {},
        "question_range": "BETWEEN 1309 AND 1365"
    },
    "OtherLifestyleFactors": {
        "dateDict": {
            "R0_AircrewTravel_Start": ["R0_AricrewTravelStartYear"],
            "R0_AircrewTravel_End": ["R0_AricrewTravelEndYear"]
        },
        "question_range": "BETWEEN 2400 AND 2433"
    }
}