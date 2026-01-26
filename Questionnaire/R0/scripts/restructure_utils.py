"""
Restructuring utilities for questionnaire ETL.

This module turns cleaned, mostly-flat records into nested JSON structures
that exactly match the section JSON Schemas.

It provides:
- Resolver cache builders that map schema fields to lists of raw variables.
- 'restructure_by_schema', which places values into the correct arrays and
  nested objects based on schema metadata and nested_utils' resolution.

The output of 'restructure_by_schema' is ready to be validated and written as the
final JSON payload for each section.
"""

from __future__ import annotations
import sys
import re
import nested_utils as nv
import os
import json
from typing import Dict, List, Tuple, Any, Optional

from nested_utils import rename_variable as _resolve

sys.path.append(os.path.abspath("N:\\CancerEpidem\\BrBreakthrough\\DeliveryProcess\Schema_and_Derivation_utils"))
from config import validation_path

CACHE_DIR = os.path.join(".", "out", "ResolverCache")

def _norm_section(s: str) -> str:
    """
    Normalise a section label into a canonical slug used by this module.
    """
    return (s or "").replace("_", "").lower()

def build_resolver_cache_from_columns(section_slug: str, q_sect, raw_columns, cache_dir: str = CACHE_DIR, validation_path = validation_path):
    """
    Build and save a resolver index for a section.

    The resolver cache maps schema leaf fields to the raw variable names
    that can populate them, optionally grouped by array index (e.g. '1',
    '2', 'A', 'B').

    This is used both to speed up restructuring and to provide a human-
    readable summary under the ValidationSummary directory.
    """
    os.makedirs(cache_dir, exist_ok=True)

    index = {}
    sect_norm = _norm_section(section_slug)
    for col in map(str, raw_columns):
        try:
            meta = _resolve(col)
        except Exception:
            meta = None
        if not meta or _norm_section(meta.get("section")) != sect_norm:
            continue

        r0 = meta.get("schema_field")
        if not r0:
            continue
        label = meta.get("index_label")
        entry = meta.get("entry_num")
        
        if label not in (None, ""):
            bucket = str(label)
        elif entry is not None:
            bucket = int(entry)
        else:
            bucket = "all"

        d = index.setdefault(r0, {})
        d.setdefault("all", []).append(col)
        d.setdefault(bucket, []).append(col)

    # sort and de-dup to keep files tidy
    for r0, buckets in index.items():
        for k, cols in list(buckets.items()):
            buckets[k] = sorted(dict.fromkeys(cols))

    os.makedirs(validation_path, exist_ok=True)

    gen_val_path = os.path.join(validation_path, f"{q_sect}_ValidationSummary")
    json_path = os.path.join(gen_val_path, f"{section_slug}_resolver_index.json")

    if not os.path.isdir(gen_val_path):
        os.makedirs(gen_val_path)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(index, f, indent=2)

    return index, json_path

def build_breast_cancer_resolver_cache(
    q_sect: str,
    raw_columns,
    section_slug: str = "breast_cancer",
    cache_dir: str = CACHE_DIR,
    validation_path = validation_path,
):
    """
    Specialised resolver cache builder for breast cancer-related sections.

    Handles the additional complexity of multiple nested arrays and
    relatives/episodes in the schema.
    """
    os.makedirs(cache_dir, exist_ok=True)

    index: dict = {}
    sect_norm = _norm_section(section_slug)

    for col in map(str, raw_columns):
        try:
            meta = _resolve(col)
        except Exception:
            meta = None

        if not meta or _norm_section(meta.get("section")) != sect_norm:
            continue

        r0 = meta.get("schema_field")
        if not r0:
            continue

        array_path = list(meta.get("array_path") or [])
        indices    = list(meta.get("indices") or [])

        # CASE 1: top-level BreastCancer fields (no array_path)
        # e.g. R0_MoreDrugEpisodesFirstBC
        if not array_path:
            label = meta.get("index_label")
            entry = meta.get("entry_num")

            if label not in (None, ""):
                bucket = str(label)
            elif entry is not None:
                try:
                    bucket = int(entry)
                except Exception:
                    bucket = str(entry)
            else:
                bucket = "all"

            d = index.setdefault(r0, {})
            d.setdefault("all", []).append(col)
            d.setdefault(bucket, []).append(col)
            continue

        # Only do nested handling for BreastCancers[*]
        if array_path[0] != "BreastCancers":
            continue

        # CASE 2: only BreastCancers[*]
        if len(array_path) == 1 or len(indices) <= 1:
            bc_idx_raw = indices[0] if indices else 1
            try:
                bc_idx = int(bc_idx_raw) if bc_idx_raw not in (None, "") else 1
            except Exception:
                bc_idx = 1

            rmap = index.setdefault(r0, {})
            rmap.setdefault("all", []).append(col)
            rmap.setdefault(bc_idx, []).append(col)
            continue

        # CASE 3: BreastCancers[*].ChildArray[*] (e.g. DrugTreatment)
        bc_idx_raw    = indices[0] if indices else 1
        child_idx_raw = indices[1] if len(indices) > 1 else None

        try:
            bc_idx = int(bc_idx_raw) if bc_idx_raw not in (None, "") else 1
        except Exception:
            bc_idx = 1

        try:
            child_idx = int(child_idx_raw) if child_idx_raw not in (None, "") else None
        except Exception:
            child_idx = None

        rmap = index.setdefault(r0, {})
        rmap.setdefault("all", []).append(col)

        outer = rmap.setdefault(bc_idx, {})
        outer.setdefault("all", []).append(col)

        if child_idx is not None:
            outer.setdefault(child_idx, []).append(col)

    for r0, mapping in index.items():
        # top-level "all"
        if "all" in mapping:
            mapping["all"] = sorted(dict.fromkeys(mapping["all"]))

        for bc_idx, val in list(mapping.items()):
            if bc_idx == "all":
                continue

            # Case 1 / 2: list of columns for this bucket
            if isinstance(val, list):
                mapping[bc_idx] = sorted(dict.fromkeys(val))
                continue

            # Case 3: nested dict for BreastCancers[*].ChildArray[*]
            if isinstance(val, dict):
                if "all" in val:
                    val["all"] = sorted(dict.fromkeys(val["all"]))
                for child_idx, cols in list(val.items()):
                    if child_idx == "all":
                        continue
                    val[child_idx] = sorted(dict.fromkeys(cols))

    os.makedirs(validation_path, exist_ok=True)
    gen_val_path = os.path.join(validation_path, f"{q_sect}_ValidationSummary")
    if not os.path.isdir(gen_val_path):
        os.makedirs(gen_val_path)

    json_path = os.path.join(gen_val_path, "breast_cancer_resolver_index.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(index, f, indent=2)

    return index, json_path

def load_resolver_cache(section_slug: str, cache_dir: str = CACHE_DIR):
    path = os.path.join(cache_dir, f"{section_slug}_resolver_index.json")
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _discover_toplevel_arrays(schema):
    out = []
    props = (schema or {}).get("properties") or {}
    for k, v in props.items():
        if isinstance(v, dict) and v.get("type") == "array":
            out.append(k)
    return out

def _index_fields_by_array(schema):
    """
    Map array name -> its index field by scanning item properties for '*Num' style fields.
    (Child arrays might not have an index field; that's fine.)
    """
    idx_map = {}
    def looks_like_index_field(key: str) -> bool:
        k = key[3:] if key.startswith("R0_") else key
        return re.search(r'Num($|_)', k, flags=re.I) is not None
    def walk(node, arrays):
        if not isinstance(node, dict): return
        props = node.get("properties", {})
        for k, v in props.items():
            if isinstance(v, dict):
                if v.get("type") == "array" and "items" in v:
                    walk(v["items"], arrays + [k])
                elif "properties" in v:
                    walk(v, arrays)
                else:
                    if arrays:
                        arr = arrays[-1]
                        if arr not in idx_map and looks_like_index_field(k):
                            idx_map[arr] = k
        if node.get("type") == "array" and "items" in node:
            walk(node["items"], arrays)
    walk(schema, [])
    return idx_map

def _has_payload(d, ignore_keys=set()):
    for k, v in d.items():
        if k in ignore_keys:
            continue
        if v not in (None, "", [], {}):
            return True
    return False

def _order_item_fields_with_extra_last(item: dict) -> dict:
    """
    Return a new dict with any '*Extra' arrays moved to the end (after scalar/parent fields).
    Preserves the original order among non-extra keys.
    """
    if not isinstance(item, dict):
        return item
    keys = list(item.keys())
    extra_keys = [k for k in keys if isinstance(item.get(k), list) and k.lower().endswith("extra")]
    base_keys = [k for k in keys if k not in extra_keys]
    ordered = {}
    for k in base_keys:
        ordered[k] = item[k]
    for k in extra_keys:
        ordered[k] = item[k]
    return ordered

def _coerce_meta_to_new_shape(meta, section_slug):
    if not meta or _norm_section(meta.get("section")) != _norm_section(section_slug):
        return [], [], None
    if "array_path" in meta and "indices" in meta and meta.get("array_path"):
        return list(meta["array_path"]), list(meta.get("indices", [])), meta.get("schema_field")
    array_name = meta.get("array_name")
    entry_num = meta.get("entry_num")
    if array_name and entry_num is not None:
        return [array_name], [int(entry_num)], meta.get("schema_field")
    return [], [], None

def _child_maxitems_map(schema):
    """
    Returns { (parent_array_name, child_array_name): maxItems or None }
    for immediate child arrays inside each parent array item.
    """
    out = {}
    props = (schema or {}).get("properties") or {}
    for arr1, arr_def in props.items():
        if isinstance(arr_def, dict) and arr_def.get("type") == "array":
            item_props = (arr_def.get("items", {}) or {}).get("properties", {}) or {}
            for k, v in item_props.items():
                if isinstance(v, dict) and v.get("type") == "array":
                    out[(arr1, k)] = (v.get("items", {}) or {}).get("maxItems")
    return out

def restructure_by_schema(
    processed_data: List[dict],
    schema: dict,
    section_slug: str,
    variable_mapping: Optional[Dict[str, dict]] = None,
    warm_resolver: bool = True,
) -> List[dict]:
    """
    Core restructuring engine.

    For each record in `processed_data`:
    - Start from a flat dict of cleaned values.
    - Use the resolver cache and `nested_utils.rename_variable` to
      determine where each raw field should live in the nested JSON.
    - Create and populate any necessary arrays and child objects.
    - Respect index fields (e.g. pregnancy number, record number) and
      schema `maxItems` constraints.
    - Return a list of objects conforming structurally to the schema.

    Parameters
    ----------
    schema : dict
        JSON Schema describing the section.
    processed_data : list[dict]
        Output of `process_nested_data` / `process_flat_data`.
    section_slug : str
        Canonical section slug (e.g. "alcohol_smoking_diet").
    q_sect : str
        Human-readable section label used in filenames.
    variable_mapping : dict, optional
        Optional overrides for raw -> schema field mapping.
    logger :
        Optional logger.
    use_cache : bool
        Whether to use or build a resolver cache on disk.
    """
    variable_mapping = variable_mapping or {}

    # schema inspection helpers 
    def _discover_toplevel_arrays(s: dict) -> List[str]:
        props = (s or {}).get("properties") or {}
        return [k for k, v in props.items() if isinstance(v, dict) and v.get("type") == "array"]

    def _build_leaf_index(s: dict) -> Dict[str, List[str]]:
        # { leaf_name -> array_path ([] | [arr1] | [arr1, arr2]) }
        out: Dict[str, List[str]] = {}
        def walk(node: dict, arrays: List[str]):
            if not isinstance(node, dict): return
            props = node.get("properties") or {}
            for k, v in props.items():
                if not isinstance(v, dict): continue
                if v.get("type") == "array" and "items" in v:
                    walk(v["items"], arrays + [k])
                elif "properties" in v:
                    walk(v, arrays)
                else:
                    out[k] = list(arrays)
        walk(s or {}, [])
        return out

    def _index_fields_by_array(s: dict) -> Dict[str, str]:
        # map array -> its index field (first *Num detected)
        idx: Dict[str, str] = {}
        def looks_like_idx(k: str) -> bool:
            k2 = k[3:] if k.startswith("R0_") else k
            return re.search(r'Num($|_)', k2, re.I) is not None
        def walk(node: dict, arrays: List[str]):
            if not isinstance(node, dict): return
            props = node.get("properties", {})
            for k, v in props.items():
                if isinstance(v, dict):
                    if v.get("type") == "array" and "items" in v:
                        walk(v["items"], arrays + [k])
                    elif "properties" in v:
                        walk(v, arrays)
                    else:
                        if arrays:
                            arr = arrays[-1]
                            if arr not in idx and looks_like_idx(k):
                                idx[arr] = k
            if node.get("type") == "array" and "items" in node:
                walk(node["items"], arrays)
        walk(s, [])
        return idx

    def _child_maxitems_map(s: dict) -> Dict[Tuple[str, str], Optional[int]]:
        out = {}
        props = (s or {}).get("properties") or {}
        for arr1, arr_def in props.items():
            if not (isinstance(arr_def, dict) and arr_def.get("type") == "array"):
                continue
            mprops = ((arr_def.get("items") or {}).get("properties") or {})
            for k, v in mprops.items():
                if isinstance(v, dict) and v.get("type") == "array":
                    mi = v.get("maxItems")
                    out[(arr1, k)] = mi if isinstance(mi, int) else None
        return out

    def _norm_section(x: str) -> str:
        return (x or "").replace("_", "").lower()

    def _coerce_meta(meta: dict, section: str) -> Tuple[List[str], List[int], Optional[str], Optional[Any]]:
        if not meta or _norm_section(meta.get("section")) != _norm_section(section):
            return [], [], None, None
        if meta.get("array_path"):
            return list(meta["array_path"]), list(meta.get("indices", [])), meta.get("schema_field"), meta.get("index_label")
        if meta.get("array_name") and meta.get("entry_num") is not None:
            return [meta["array_name"]], [int(meta["entry_num"])], meta.get("schema_field"), meta.get("index_label")
        return [], [], meta.get("schema_field"), meta.get("index_label")

    # precompute schema facts 
    leaf_index = _build_leaf_index(schema)     
    schema_leaves = set(leaf_index.keys())
    toplevel_arrays = _discover_toplevel_arrays(schema)
    index_field_by_array = _index_fields_by_array(schema)
    child_max = _child_maxitems_map(schema)
    has_preg = "Pregnancies" in toplevel_arrays

    if warm_resolver:
        all_keys = {k for rec in (processed_data or []) for k in rec if k != "R0_StudyID"}
        unresolved = [k for k in all_keys if k not in schema_leaves and k not in variable_mapping]
        try:
            if hasattr(nv, "build_resolver_cache_from_columns"):
                nv.build_resolver_cache_from_columns(section_slug, q_sect, unresolved)
        except Exception:
            pass  # safe no-op

    # pre-resolve unique unresolved raw keys ONCE 
    resolved_meta: Dict[str, Tuple[List[str], List[int], Optional[str], Optional[Any]]] = {}
    for rec in (processed_data or []):
        for raw in rec.keys():
            if raw == "R0_StudyID" or raw in schema_leaves or raw in variable_mapping or raw in resolved_meta:
                continue
            try:
                m = nv.rename_variable(raw)
            except Exception:
                m = None
            resolved_meta[raw] = _coerce_meta(m, section_slug) if m else ([], [], None, None)

    out: List[dict] = []

    # helpers for child accumulation 
    def _place_final_leaf(obj: dict, level1: dict, leaf: str, val: Any) -> None:
        ap = leaf_index.get(leaf, [])
        if not ap:
            obj[leaf] = val
            return
        if len(ap) == 1:
            arr1 = ap[0]
            parent = level1.setdefault(arr1, {}).setdefault(1, {})
            idx1 = index_field_by_array.get(arr1)
            if idx1 and idx1 not in parent:
                parent[idx1] = 1
            parent[leaf] = val
            return
        if len(ap) == 2:
            arr1, arr2 = ap
            parent = level1.setdefault(arr1, {}).setdefault(1, {})
            idx1 = index_field_by_array.get(arr1)
            if idx1 and idx1 not in parent:
                parent[idx1] = 1
            cmap = parent.setdefault(f"__child_map__:{arr2}", {})
            key = max(cmap.keys(), default=0) + 1
            child = cmap.setdefault(key, {})
            child[leaf] = val

    def _append_child(parent: dict, arr2: str, idx_field2: Optional[str], idx2: int, field: str, val: Any):
        cmap = parent.setdefault(f"__child_map__:{arr2}", {})
        key = int(idx2) if idx_field2 else (idx2 if idx2 > 0 else (max(cmap.keys(), default=0) + 1))
        child = cmap.setdefault(key, {})
        child[field] = val

    def _finalize_children(item: dict, arr1: str):
        maps = [(k, v) for k, v in list(item.items()) if isinstance(k, str) and k.startswith("__child_map__:")]
        for k, cmap in maps:
            arr2 = k.split(":", 1)[1]
            idx_field2 = index_field_by_array.get(arr2)
            items = []
            for key in sorted(cmap.keys()):
                ch = cmap[key]
                if idx_field2 and idx_field2 not in ch:
                    ch[idx_field2] = key
                # drop empty children
                payload = {kk: vv for kk, vv in ch.items() if kk != idx_field2 and vv not in (None, "", [], {})}
                if payload or (idx_field2 and ch.get(idx_field2) not in (None, "", [], {})):
                    items.append(ch)
            mi = child_max.get((arr1, arr2))
            if isinstance(mi, int) and mi >= 0:
                items = items[:mi]
            item[arr2] = items
            del item[k]

    def _has_payload(d: dict, ignore: set[str] | None = None) -> bool:
        ign = ignore or set()
        for k, v in d.items():
            if k in ign: continue
            if isinstance(v, dict):
                if _has_payload(v, ign): return True
            elif isinstance(v, list):
                for ch in v:
                    if isinstance(ch, dict) and _has_payload(ch, ign): return True
            else:
                if v not in (None, "", [], {}): return True
        return False

    def _order_extras_last(item: dict) -> dict:
        keys = list(item.keys())
        extra = [k for k in keys if isinstance(item.get(k), list) and k.lower().endswith("extra")]
        base = [k for k in keys if k not in extra]
        out = {k: item[k] for k in base}
        out.update({k: item[k] for k in extra})
        return out

    # main loop 
    for rec in (processed_data or []):
        if not isinstance(rec, dict):
            continue

        obj: Dict[str, Any] = {"R0_StudyID": rec.get("R0_StudyID")}
        level1: Dict[str, Dict[int, dict]] = {arr: {} for arr in toplevel_arrays}

        for raw, val in rec.items():
            if raw == "R0_StudyID":
                continue

            # Fast path: already a schema leaf (final name)
            if raw in schema_leaves:
                _place_final_leaf(obj, level1, raw, val)
                continue

            # Mapping override
            mapped = variable_mapping.get(raw)
            if isinstance(mapped, dict):
                ap = list(mapped.get("array_path") or [])
                idxs = list(mapped.get("indices") or [])
                field = mapped.get("schema_field")
                index_label = None
            else:
                ap, idxs, field, index_label = resolved_meta.get(raw, ([], [], None, None))

            if not field:
                # Unknown or out-of-section raw key: ignore silently
                continue

            if not ap:
                # top-level scalar
                obj[field] = val
                continue

            # pregnancies normalization (preserve legacy behavior if used)
            arr1 = ap[0]
            if has_preg and isinstance(field, str) and field.startswith("R0_Preg_") and arr1 not in level1:
                arr1 = "Pregnancies"

            # parent
            idx1 = int(idxs[0] if len(idxs) >= 1 else 1)
            if idx1 < 1: idx1 = 1
            parent = level1.setdefault(arr1, {}).setdefault(idx1, {})
            idx_field1 = index_field_by_array.get(arr1)
            if idx_field1 and idx_field1 not in parent:
                parent[idx_field1] = index_label if index_label is not None else idx1

            if len(ap) == 1:
                parent[field] = val
            elif len(ap) == 2:
                arr2 = ap[1]
                idx2 = int(idxs[1] if len(idxs) >= 2 else 1)
                if idx2 < 1: idx2 = 1
                idx_field2 = index_field_by_array.get(arr2)
                _append_child(parent, arr2, idx_field2, idx2, field, val)
            else:
                # deeper nesting not expected
                continue

        # finalize each top-level array
        for arr in toplevel_arrays:
            items = []
            for idx1 in sorted(level1.get(arr, {})):
                item = level1[arr][idx1]
                _finalize_children(item, arr)
                item = _order_extras_last(item)
                idx_field1 = index_field_by_array.get(arr)
                ignore = {idx_field1} if idx_field1 else set()
                if _has_payload(item, ignore):
                    items.append(item)
            obj[arr] = items

        out.append(obj)

    return out