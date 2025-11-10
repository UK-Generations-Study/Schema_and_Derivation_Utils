"""
Quality control utilities for questionnaire ETL.

This module provides tools to:
- Compare value distributions before and after cleaning/pseudo-anonymisation.
- Reconcile differences using the change-tracking logs from processing.
- Generate simple histogram PDFs for manual inspection.
- Check coverage of variables between raw input and final JSON output.

It is intended to be used from the `*_Load.ipynb` notebooks to verify
that the ETL behaves as expected for each questionnaire section.
"""

from __future__ import annotations
import json, os, math, re
from collections import defaultdict, Counter
from typing import Dict, List, Tuple, Any, Optional, Iterable, Set, Union
from pathlib import Path

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from nested_utils import rename_variable
from pseudo_anon_utils import dateDict as PSEUDO_DATECFG

# Generic helpers

_NULL_TOKENS = {"", "NA", "N/A", "NULL", "null", "??", "NK"}

def _canon_val(x):
    """
    Canonicalise a value for comparison purposes.

    E.g. normalises whitespace and case for strings so that frequency
    comparisons are robust to tiny formatting changes.
    """
    if x is None:
        return None
    try:
        if isinstance(x, float) and math.isnan(x):
            return None
    except Exception:
        pass
    s = str(x).strip()
    if s in _NULL_TOKENS:
        return None
    try:
        if re.fullmatch(r"[+-]?\d+(\.0+)?", s):
            return int(float(s))
        if re.fullmatch(r"[+-]?\d+\.\d+", s):
            f = float(s)
            return int(f) if f.is_integer() else f
    except Exception:
        pass
    return s

def _value_counts_canon(series: pd.Series) -> Dict[Any, int]:
    """
    Compute value counts on a canonicalised version of a Series.

    Used to compare frequencies before/after cleaning without being
    overly sensitive to harmless string formatting differences.
    """
    ser = series.map(_canon_val)
    vc = ser.value_counts(dropna=False)
    out = {}
    for k, v in vc.items():
        key = None
        if k is not None and not (isinstance(k, float) and math.isnan(k)):
            key = k
        out[key] = int(v)
    return out

def _canon_key_for_json(k):
    return "null" if k is None else str(k)

def _apply_change_chain(orig_counts_canon: dict, change_map: dict) -> dict:
    """
    Apply a change map {old_val: {new_value: X, count: N}, ...} to the
    canonicalized original frequencies. Returns expected frequencies.
    """
    edges = {}
    for orig_val, info in (change_map or {}).items():
        try:
            o = _canon_val(orig_val)
            n = _canon_val(info.get("new_value"))
            c = int(info.get("count", 0))
        except Exception:
            continue
        edges.setdefault(o, Counter())
        edges[o][n] += c

    expected = Counter(orig_counts_canon)

    for o, count_o in list(orig_counts_canon.items()):
        if o not in edges:
            continue
        expected[o] -= count_o
        pushed = sum(edges[o].values())
        for n, c in edges[o].items():
            expected[n] += c
        remainder = count_o - pushed
        if remainder > 0:
            expected[o] += remainder

    return {k: int(v) for k, v in expected.items() if int(v) != 0}

def _select_raws_for_instance(resolver_pairs, r0_key: str, inst_label: str | None):
    pairs = resolver_pairs.get(r0_key, [])
    if inst_label is not None:
        specific = [raw for raw, lbl in pairs if lbl == str(inst_label)]
        if specific:
            return specific
    return [raw for raw, lbl in pairs if lbl is None]

def _deep_iter_fields(record: Any, prefix: str = "") -> Iterable[str]:
    """
    Yield all leaf keys from a nested dict/list structure as 'path.to.key' JSON keys (schema keys).
    Only yields schema JSON keys (not the "name" aliases).
    """
    if isinstance(record, dict):
        for k, v in record.items():
            if k == "R0_StudyID":
                continue
            path = f"{prefix}.{k}" if prefix else k
            if isinstance(v, (dict, list)):
                yield from _deep_iter_fields(v, path)
            else:
                yield path
    elif isinstance(record, list):
        for item in record:
            yield from _deep_iter_fields(item, prefix)


def _deep_collect_schema_leaves(data: List[dict]) -> Set[str]:
    leaves: Set[str] = set()
    for rec in data or []:
        for leaf in _deep_iter_fields(rec, ""):
            leaf_key = leaf.split(".")[-1]
            leaves.add(leaf_key)
    return leaves


def _load_json(path: Union[str, os.PathLike]) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _maybe_load_change_tracking(ct_path: Optional[str], section_name: str) -> Optional[dict]:
    if not ct_path:
        return None
    file_path = os.path.join(ct_path, f"{section_name}_ChangeTracking.json")
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def _flatten_series(s: pd.Series) -> Counter:
    def norm(v):
        if v is None:
            return None
        if isinstance(v, str):
            t = v.strip()
            if t == "" or t.lower() == "null":
                return None
            return t
        return v
    return Counter([norm(x) for x in s.values.tolist()])


def _histogram(ax, values, title: str):
    vals = [v for v in values if v is not None and v != ""]
    if not vals:
        ax.text(0.5, 0.5, "No data", ha="center", va="center")
        ax.set_title(title)
        return
    is_numeric = all(_is_numberish(v) for v in vals)
    if is_numeric:
        arr = np.array([float(v) for v in vals], dtype=float)
        ax.hist(arr, bins="auto")
    else:
        cnt = Counter(vals)
        top = cnt.most_common(25)
        labels, freqs = zip(*top)
        xpos = np.arange(len(labels))
        ax.bar(xpos, freqs)
        ax.set_xticks(xpos)
        ax.set_xticklabels(labels, rotation=90)
    ax.set_title(title)


def _is_numberish(x) -> bool:
    try:
        float(str(x).strip())
        return True
    except Exception:
        return False


def _resolver_pairs_from_index(resolver_index: dict) -> Dict[str, List[Tuple[str, Optional[str]]]]:
    """
    Accept resolver index shaped like:
      {
        "R0_AgeBreastDev": {
           "all": ["Q3_16"],
           "7": ["Q3_x_for_inst7"],  # optional instance-labeled buckets
           "11": ["Q3_x_for_inst11"]
        },
        ...
      }

    MUST include the 'all' bucket (label=None) so flat mappings aren't dropped.
    """
    out: Dict[str, List[Tuple[str, Optional[str]]]] = defaultdict(list)
    for r0, buckets in (resolver_index or {}).items():
        for bucket, raw_list in (buckets or {}).items():
            # Treat 'all' (and any non-string) as unlabeled / no instance
            label: Optional[str] = str(bucket) if isinstance(bucket, str) and bucket.lower() != "all" else None
            for raw in (raw_list or []):
                raw_name = str(raw).strip()
                if raw_name:
                    out[r0].append((raw_name, label))
    return out


def _split_instance_from_processed_name(name: str) -> Tuple[str, Optional[str]]:
    m = re.match(r'^(R0_[A-Za-z0-9]+?)(?:[_]?([A-Za-z0-9]+))?$', str(name or ""))
    if not m:
        return name, None
    base, inst = m.group(1), m.group(2)
    return base, inst


def _collect_pii_vars(dfPII):
    """
    Accepts:
      • a set/tuple/list of names or dicts
      • a dict with key 'removed_pii_vars' (value can be list or set)
      • a pandas Series or DataFrame (with columns 'VariableName' or 'name')
    Returns a set[str] of variable names.
    """
    if dfPII is None:
        return set()

    # Normalize to an iterable of items
    items = None
    try:
        import pandas as _pd
    except Exception:
        _pd = None

    if isinstance(dfPII, dict):
        items = dfPII.get("removed_pii_vars", [])
    elif isinstance(dfPII, (list, set, tuple)):
        items = dfPII
    elif _pd is not None and isinstance(dfPII, _pd.Series):
        items = dfPII.tolist()
    elif _pd is not None and isinstance(dfPII, _pd.DataFrame):
        if "VariableName" in dfPII.columns:
            items = dfPII["VariableName"].tolist()
        elif "name" in dfPII.columns:
            items = dfPII["name"].tolist()
        else:
            # Best-effort: flatten the first column
            items = dfPII.iloc[:, 0].tolist()
    else:
        # Unknown shape → nothing to collect
        return set()

    out = set()
    for v in items or []:
        if isinstance(v, str):
            name = v.strip()
            if name:
                out.add(name)
        elif isinstance(v, dict):
            name = str(v.get("VariableName") or v.get("name") or "").strip()
            if name:
                out.add(name)
        # ignore other types quietly
    return out



def _ensure_validation_dir(base_dir: str) -> str:
    """
    Ensure a "_validation summary" directory exists under base_dir and return its path.
    If base_dir already ends with "_validation summary", use it directly.
    """
    if base_dir is None:
        return None
    out = os.path.abspath(base_dir)
    return out


# Variable presence + accounting

def qc_check_variables(
    raw_pivot_df: pd.DataFrame,
    processed_json: List[dict],
    resolver_index: Union[str, dict],
    dfPII: Optional[pd.DataFrame] = None,
    section_name: str = None,
    schema: Optional[dict] = None,
    datecfg: Optional[dict] = None,
    save_to: Optional[str] = None,
) -> dict:
    """
    High-level QC entry point for a section.

    Compares:
    - Raw pivoted values vs. values present in the final JSON.
    - Counts of non-null values for each variable.
    - Mapping of raw variable names to schema fields via nested_utils.

    Uses the change-tracking information to explain differences and
    highlights variables where the ETL may have dropped or altered
    values unexpectedly.
    """
    if isinstance(resolver_index, (str, os.PathLike)):
        resolver_index = _load_json(resolver_index)

    raw_fields = sorted([str(c) for c in raw_pivot_df.columns])
    processed_leaves = sorted(_deep_collect_schema_leaves(processed_json))

    pairs = _resolver_pairs_from_index(resolver_index)

    raw_to_proc: Dict[str, Set[str]] = defaultdict(set)
    for r0, lst in pairs.items():
        for raw, _lab in lst:
            raw_to_proc[raw].add(r0)

    matched_raw = set()
    unmatched_raw = set()
    for raw in raw_fields:
        procs = raw_to_proc.get(raw, set())
        if any(p in processed_leaves for p in procs):
            matched_raw.add(raw)
        else:
            unmatched_raw.add(raw)

    #  Account for PII, aggregated dates, and StudyID→TCode replacement 

    # Build resolver pairs once: {processed_r0: [(raw_name, label_or_None), ...]}
    pairs = _resolver_pairs_from_index(resolver_index)

    # PII accounting (resolver-aware)
    # Collect names from dfPII (strings or dicts). These may be raw or processed.
    pii_names = _collect_pii_vars(dfPII)

    # Map any processed PII names to their raw components via resolver;
    # keep direct raw names that exist in the raw pivot.
    pii_raw: set[str] = set()
    for name in pii_names:
        # take direct raw if present
        if name in raw_pivot_df.columns:
            pii_raw.add(name)
        # if it's a processed name, resolve to raw(s)
        for raw, _lab in pairs.get(name, []):
            pii_raw.add(raw)

    accounted_pii = sorted([r for r in unmatched_raw if r in pii_raw])

    # Aggregated date components accounting (resolver-aware)
    # Pull the per-section date dict from pseudo_anon_utils.dateDict
    # Keys are processed derived fields; values are lists of components (raw or processed).
    datecfg = datecfg or PSEUDO_DATECFG or {}
    section_dates = (datecfg.get(section_name) or {}).get("dateDict", {})

    # Only account components if the aggregated processed field actually exists
    processed_leaves = sorted(_deep_collect_schema_leaves(processed_json))

    accounted_dates_raw: set[str] = set()
    for derived_proc, components in (section_dates or {}).items():
        if derived_proc not in processed_leaves:
            continue
        for comp in (components or []):
            if comp in raw_pivot_df.columns:
                accounted_dates_raw.add(comp)
            for raw, _lab in pairs.get(comp, []):
                accounted_dates_raw.add(raw)

    accounted_dates = sorted([r for r in unmatched_raw if r in accounted_dates_raw])

    # StudyID is accounted if TCode present
    # If the processed JSON contains R0_TCode, then any raw "StudyID" should be treated as accounted.
    accounted_ids: set[str] = set()
    if "R0_TCode" in processed_leaves and "StudyID" in unmatched_raw:
        accounted_ids.add("StudyID")

    # Final unaccounted
    unaccounted = sorted([
        r for r in unmatched_raw
        if r not in pii_raw and r not in accounted_dates_raw and r not in accounted_ids
    ])

    # Replace the old assignments feeding the report:
    accounted_pii = sorted([r for r in unmatched_raw if r in pii_raw])
    accounted_dates = sorted([r for r in unmatched_raw if r in accounted_dates_raw])


    report = {
        "summary": {
            "raw_field_count": len(raw_fields),
            "processed_leaf_count": len(processed_leaves),
            "matched_raw": len(matched_raw),
            "unmatched_raw": len(unmatched_raw),
            "accounted_pii": len(accounted_pii),
            "accounted_dates": len(accounted_dates),
            "unaccounted": len(unaccounted),
            "status": "PASSED" if len(unaccounted) == 0 else "WARNING"
        },
        "details": {
            "raw_fields": raw_fields,
            "processed_leaves": processed_leaves,
            "unmatched_raw": sorted(unmatched_raw),
            "accounted_pii": accounted_pii,
            "accounted_dates": accounted_dates,
            "unaccounted": unaccounted
        }
    }

    # optional write-out
    if save_to:
        out_dir = _ensure_validation_dir(save_to)
        if out_dir:
            out_path = os.path.join(out_dir, 'variable_check.json')
            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            report['written_to'] = out_path
    return report


def _collect_values_for_processed_leaf(
    processed_json: List[dict],
    r0_leaf: str,
    schema: Optional[dict] = None
) -> List[Any]:
    """Collect *values* for a given processed leaf.

    Supports instance-suffixed variables like `R0_XrayAge_4` by:
      • using the JSON Schema to locate the array container and its
        discriminator field (e.g., `R0_Xray_Num` for XrayEvents),
      • then, for the matching array item, deep-walking into nested
        structures (like XrayEventsExtra) to find the base leaf.

    If the array instance doesn't exist for a record, we treat the leaf
    as NULL for that record (so nulls are included in frequencies).

    Ignores R0_StudyID / R0_TCode entirely.
    """
    vals: List[Any] = []

    def _deep_get_first(node: Any, leaf_key: str) -> Any:
        """Return the first occurrence of a leaf_key in a nested dict/list."""
        if isinstance(node, dict):
            if leaf_key in node and not isinstance(node[leaf_key], (dict, list)):
                return node[leaf_key]
            for v in node.values():
                res = _deep_get_first(v, leaf_key)
                if res is not None:
                    return res
        elif isinstance(node, list):
            for it in node:
                res = _deep_get_first(it, leaf_key)
                if res is not None:
                    return res
        return None

    inst_label: Optional[str] = None
    base_leaf = r0_leaf
    container_prop = discrim_field = None
    discrim_type = None

    # Detect instance-suffixed variables like 'R0_XrayAge_4'
    if schema and "_" in r0_leaf:
        base_candidate, inst_candidate = r0_leaf.rsplit("_", 1)
        arr_info = _find_array_container_and_discriminator(schema, base_candidate)
        if arr_info is not None:
            base_leaf = base_candidate
            inst_label = inst_candidate
            container_prop, discrim_field, discrim_type = arr_info

    # Instance-aware path: one value per record (value or NULL)
    if inst_label is not None and container_prop is not None and discrim_field is not None:
        for rec in processed_json or []:
            if not isinstance(rec, dict):
                vals.append(None)
                continue

            arr = rec.get(container_prop, [])
            if not isinstance(arr, list):
                vals.append(None)
                continue

            # Determine how to coerce the instance label (int vs str)
            example_key_val = None
            for item in arr:
                if isinstance(item, dict) and discrim_field in item:
                    example_key_val = item.get(discrim_field)
                    if example_key_val is not None:
                        break
            coerced_inst = _coerce_instance_label(inst_label, example_key_val)

            found_value = False
            for item in arr:
                if not isinstance(item, dict):
                    continue
                if item.get(discrim_field) == coerced_inst:
                    # Deep search within the matching item so we can find
                    # things like XrayEventsExtra[0].R0_XrayAge
                    val = _deep_get_first(item, base_leaf)
                    vals.append(val)
                    found_value = True
                    break

            if not found_value:
                # Missing instance entirely → treat as NULL for this record
                vals.append(None)

        return vals

    # Fallback: non-instance leaf, deep-walk the whole record tree
    def walk(node: Any):
        if isinstance(node, dict):
            for k, v in node.items():
                if k in {"R0_StudyID", "R0_TCode"}:
                    continue
                if isinstance(v, (dict, list)):
                    walk(v)
                else:
                    if k == r0_leaf:
                        vals.append(v)
        elif isinstance(node, list):
            for it in node:
                walk(it)

    for rec in processed_json or []:
        walk(rec)
    return vals

def _collect_breastcancer_drug_ongoing_values(
    processed_json: List[dict],
    bc_inst_label: str,
    drug_inst_label: str,
) -> List[Any]:
    """
    BreastCancer-specific: collect R0_BCDrugRegimenOngoing values for a given
    breast-cancer instance (R0_BreastCancer_Num) AND drug-treatment instance
    (R0_DrugTreatment_Num).

    Returns one value per participant (including NULL when the specific
    BC/drug episode does not exist).

    Need this bespoke functoin as breast cancers is the only true nested json file
    with multiple drug regimen instances occuring for each cancer instance.
    """
    def _coerce(label):
        try:
            return int(label)
        except Exception:
            return label

    bc_inst = _coerce(bc_inst_label)
    drug_inst = _coerce(drug_inst_label)

    vals: List[Any] = []
    for rec in processed_json or []:
        if not isinstance(rec, dict):
            vals.append(None)
            continue

        bcs = rec.get("BreastCancers", [])
        if not isinstance(bcs, list):
            vals.append(None)
            continue

        # Find the BreastCancers item with this BC instance
        bc_item = None
        for bc in bcs:
            if isinstance(bc, dict) and bc.get("R0_BreastCancer_Num") == bc_inst:
                bc_item = bc
                break

        if not bc_item:
            vals.append(None)
            continue

        drug_arr = bc_item.get("DrugTreatment", [])
        if not isinstance(drug_arr, list):
            vals.append(None)
            continue

        # Find the DrugTreatment item with this drug instance
        dt_item = None
        for dt in drug_arr:
            if isinstance(dt, dict) and dt.get("R0_DrugTreatment_Num") == drug_inst:
                dt_item = dt
                break

        if not dt_item:
            vals.append(None)
            continue

        vals.append(dt_item.get("R0_BCDrugRegimenOngoing"))
    return vals

def _find_array_container_and_discriminator(schema: dict, base_leaf: str) -> Optional[Tuple[str, str, type]]:
    """Given a base processed leaf (e.g., 'R0_HghtComp' or 'R0_XrayAge'),
    locate which top-level array contains it and the name/type of its discriminator
    (e.g., ('HeightComparison', 'R0_HghtComp_Num', int) or
           ('XrayEvents', 'R0_Xray_Num', int)).

    Also supports leaves that live inside a nested array
    under the array item (e.g. XrayEvents → XrayEventsExtra → R0_XrayAge).
    Returns None if the leaf is not inside an array.
    """
    try:
        props = (schema or {}).get("properties", {})
        for container, meta in props.items():
            if not isinstance(meta, dict) or meta.get("type") != "array":
                continue

            items = meta.get("items", {})
            if not isinstance(items, dict):
                continue

            iprops = items.get("properties", {}) if isinstance(items, dict) else {}
            if not isinstance(iprops, dict):
                iprops = {}

            # 1) Direct hit: base_leaf is a property of the array item
            found = base_leaf in iprops

            # 2) Nested hit: base_leaf is inside a nested array under the item
            if not found:
                for pname, pmeta in iprops.items():
                    if not isinstance(pmeta, dict):
                        continue
                    if pmeta.get("type") == "array":
                        sitems = pmeta.get("items", {})
                        sprops = sitems.get("properties", {}) if isinstance(sitems, dict) else {}
                        if isinstance(sprops, dict) and base_leaf in sprops:
                            found = True
                            break

            if not found:
                continue

            # Prefer a sibling discriminator on the array item
            candidates: List[str] = []
            for k, v in ipprops.items() if False else iprops.items():
                if k == base_leaf:
                    continue
                if not isinstance(v, dict):
                    continue
                if k.endswith("_Num") or "enum" in v:
                    candidates.append(k)

            # Try to infer type from enum if present
            for k in candidates:
                v = iprops[k]
                enum = v.get("enum")
                if isinstance(enum, list) and enum:
                    ty = type(enum[0])
                    return container, k, ty

            # Fallback: still return something if we found at least one candidate
            if candidates:
                k = candidates[0]
                return container, k, str

        return None
    except Exception:
        return None


def _coerce_instance_label(label: str, example_value: Any) -> Any:
    """Coerce resolver instance labels (e.g., '11', '7', 'Cur', '20')
    to the same type as the discriminator values present in data.
    """
    if isinstance(example_value, int):
        try:
            return int(label)
        except Exception:
            return example_value
    return label


def _resolver_pairs_instance_expanded(resolver_index: dict) -> Dict[str, List[str]]:
    """
    Expand resolver to map processed leaves to *raw* names with instance suffixes.

    Normal shape:
      { "R0_X": { "all": [...], "1": [...], "2": [...] } }

    BreastCancer nested shape (for DrugTreatment etc.):
      { "R0_BCDrugRegimenOngoing": {
          "all": [...],
          1: { "all": [...], 1: [...], 2: [...] },
          2: { "all": [...], 1: [...] },
          ...
        }
      }

    For nested dict buckets we use the inner 'all' list (or, if that is missing,
    the union of all inner lists).
    """
    out: Dict[str, List[str]] = {}
    for r0_leaf, mapping in (resolver_index or {}).items():
        if r0_leaf in {"R0_StudyID", "R0_TCode"}:
            continue
        if not isinstance(mapping, dict):
            continue

        # any key other than 'all' is an instance bucket
        instance_keys = [k for k in mapping.keys() if k != "all"]

        if instance_keys:
            for inst in instance_keys:
                bucket = mapping.get(inst) or []

                # BreastCancer nested case: bucket is a dict with 'all' + child indices
                if isinstance(bucket, dict):
                    if isinstance(bucket.get("all"), list):
                        raws = bucket["all"]
                    else:
                        # fallback: flatten any list-valued inner buckets
                        raws: List[str] = []
                        for v in bucket.values():
                            if isinstance(v, list):
                                raws.extend(v)
                else:
                    # normal case: bucket is already a list of raw names
                    raws = bucket or []

                if not raws:
                    continue

                out[f"{r0_leaf}_{inst}"] = list(dict.fromkeys(map(str, raws)))
        else:
            raws = mapping.get("all", []) or []
            if raws:
                out[r0_leaf] = list(dict.fromkeys(map(str, raws)))

    return out

def _reconcile_breast_cancer_drug_ongoing(
    raw_pivot_df: pd.DataFrame,
    processed_json: List[dict],
    resolver_index: dict,
    *,
    verbose: bool = False,
) -> Dict[str, dict]:
    """
    Build reconciliation entries for R0_BCDrugRegimenOngoing per
    (R0_BreastCancer_Num, R0_DrugTreatment_Num) episode.

    Uses the nested breast-cancer resolver structure:

        "R0_BCDrugRegimenOngoing": {
          "all": [...],
          "1": { "all": [...], "3": ["brcactendstill3"], ... },
          "2": { "all": [...], "1": ["brcasecct1still"], ... },
          "3": { "all": [...], "2": ["brca3ct2still"] }
        }

    and produces QC variables like:
        R0_BCDrugRegimenOngoing_1_3,
        R0_BCDrugRegimenOngoing_1_4, ...
    """
    results: Dict[str, dict] = {}

    leaf = "R0_BCDrugRegimenOngoing"
    mapping = (resolver_index or {}).get(leaf)
    if not isinstance(mapping, dict):
        return results

    def _sort_inst_keys(keys):
        def keyfn(k):
            try:
                return (0, int(k))
            except Exception:
                return (1, str(k))
        return sorted([k for k in keys if k != "all"], key=keyfn)

    bc_keys = _sort_inst_keys(mapping.keys())

    for bc_key in bc_keys:
        per_bc = mapping.get(bc_key)
        if not isinstance(per_bc, dict):
            continue

        drug_keys = _sort_inst_keys(per_bc.keys())
        for drug_key in drug_keys:
            raw_names = per_bc.get(drug_key) or []
            if isinstance(raw_names, str):
                raw_names = [raw_names]

            # ORIGINAL (raw) frequencies for all mapped raw columns
            per_raw_original: Dict[str, Dict[Any, int]] = {}
            for raw_name in raw_names:
                if raw_name in raw_pivot_df.columns:
                    per_raw_original[raw_name] = _value_counts_canon(raw_pivot_df[raw_name])

            if not per_raw_original:
                continue

            # Sum original frequencies across mapped raws
            sum_original: Dict[Any, int] = defaultdict(int)
            for freq in per_raw_original.values():
                for v, c in freq.items():
                    sum_original[v] += c

            expected = dict(sum_original)

            # PROCESSED values from BreastCancers[*].DrugTreatment[*]
            vals = _collect_breastcancer_drug_ongoing_values(
                processed_json, bc_inst_label=str(bc_key), drug_inst_label=str(drug_key)
            )
            actual = _value_counts_canon(pd.Series(vals))

            keys_all = set(expected.keys()) | set(actual.keys())
            discrepancies = {
                ("null" if k is None else str(k)): {
                    "expected": expected.get(k, 0),
                    "actual": actual.get(k, 0),
                }
                for k in keys_all
                if expected.get(k, 0) != actual.get(k, 0)
            }

            is_perfect = len(discrepancies) == 0
            proc_name = f"{leaf}_{bc_key}_{drug_key}"

            if verbose and not is_perfect:
                print(f"[reconcile][BC Ongoing] MISMATCH {proc_name} → "
                      f"preview {dict(list(discrepancies.items())[:5])}")

            results[proc_name] = {
                "original_frequencies": _jsonify_freq(dict(sum_original)),
                "expected_frequencies": _jsonify_freq(expected),
                "actual_frequencies": _jsonify_freq(actual),
                "discrepancies": discrepancies,
                "perfect_match": is_perfect,
            }

    return results

def _jsonify_freq(freq: Dict[Any, int]) -> Dict[str, int]:
    """Convert canonical keys (including None) into json-friendly string keys."""
    out: Dict[str, int] = {}
    for k, v in (freq or {}).items():
        key = "null" if k is None else str(k)
        out[key] = int(v)
    return out


def reconcile_value_frequencies(
    raw_pivot_df: pd.DataFrame,
    processed_json: List[dict],
    resolver_index: Union[str, dict],
    change_tracking: Optional[Union[str, dict]] = None,
    *,
    section_name: str = None,
    schema: Optional[dict] = None,
    variable_check: Optional[Union[str, dict]] = None,
    save_to: Optional[str] = None,
    verbose: bool = False,
) -> dict:
    """
    Attempt to explain differences in value frequencies using change-tracking.

    For a given field:
    - Canonicalise both original and final values.
    - Apply the recorded changes (old -> new) to reconstruct how many
      times each original value was transformed.
    - Identify residual discrepancies that cannot be explained by the
      documented changes.

    This helps catch unintended transformations or dropped values.
    """

    # Load inputs
    if isinstance(resolver_index, (str, os.PathLike)):
        with open(resolver_index, "r", encoding="utf-8") as f:
            resolver_index = json.load(f)

    if schema is None:
        schema_path = os.path.join(os.path.dirname(__file__) if "__file__" in globals() else os.getcwd(), f"{section_name}_Schema.json")
        if os.path.exists(schema_path):
            try:
                with open(schema_path, "r", encoding="utf-8") as f:
                    schema = json.load(f)
            except Exception:
                schema = None

    # change_tracking can be a path to either the JSON file or a folder containing <section>_ChangeTracking.json
    if isinstance(change_tracking, (str, os.PathLike)):
        ct_path = str(change_tracking)
        if os.path.isdir(ct_path):
            ct_file = os.path.join(ct_path, f"{section_name}_ChangeTracking.json")
        else:
            ct_file = ct_path
        if os.path.exists(ct_file):
            with open(ct_file, "r", encoding="utf-8") as f:
                change_tracking = json.load(f)
        else:
            change_tracking = {}
    elif change_tracking is None:
        change_tracking = {}

    # Build instance-expanded resolver pairs
    if verbose:
        print("[reconcile] Building instance-expanded resolver pairs…")
    pairs = _resolver_pairs_instance_expanded(resolver_index)

    # Determine processed leaves directly from resolver (so arrays split into suffixed leaves)
    processed_leaves: Set[str] = set(pairs.keys())

    #  Build skip lists based on variable_check accounted lists 
    accounted_pii_raw: Set[str] = set()
    accounted_dates_raw: Set[str] = set()
    if isinstance(variable_check, (str, os.PathLike)):
        try:
            with open(variable_check, "r", encoding="utf-8") as f:
                variable_check = json.load(f)
        except Exception:
            variable_check = {}
    if isinstance(variable_check, dict):
        accounted_pii_raw.update(variable_check.get("accounted_pii", []) or [])
        accounted_dates_raw.update(variable_check.get("accounted_dates", []) or [])
        # Also check common nesting like {"VariablesCheck": {"accounted_pii": [...]}}
        vc_stage = variable_check.get("VariablesCheck") or variable_check.get("variables_check") or {}
        accounted_pii_raw.update(vc_stage.get("accounted_pii", []) or [])
        accounted_dates_raw.update(vc_stage.get("accounted_dates", []) or [])
        # Also check under 'details' like in variable_check.json
        details = variable_check.get("details") or {}
        accounted_pii_raw.update(details.get("accounted_pii", []) or [])
        accounted_dates_raw.update(details.get("accounted_dates", []) or [])

    # Map raw→processed via resolver pairs (instance-expanded) so we can skip the right processed leaves
    raw_to_proc: Dict[str, Set[str]] = defaultdict(set)
    for proc, raws in pairs.items():
        for r in raws:
            raw_to_proc[r].add(proc)

    if verbose:
        print(f"[reconcile] accounted_pii raw count: {len(accounted_pii_raw)}")
        print(f"[reconcile] accounted_dates raw count: {len(accounted_dates_raw)}")

    pii_processed: Set[str] = set()
    for r in accounted_pii_raw:
        pii_processed.update(raw_to_proc.get(r, set()))

    aggregated_processed: Set[str] = set()
    for r in accounted_dates_raw:
        aggregated_processed.update(raw_to_proc.get(r, set()))

    # Build reason map for diagnostics
    proc_skip_reason: Dict[str, str] = {}
    for p in pii_processed:
        proc_skip_reason[p] = (proc_skip_reason.get(p, "") + (";" if proc_skip_reason.get(p) else "") + "PII")
    for p in aggregated_processed:
        reason = proc_skip_reason.get(p, "")
        proc_skip_reason[p] = (reason + (";" if reason else "") + "AGG_DATE")

    skip_processed: Set[str] = set()
    skip_processed.update(pii_processed)
    skip_processed.update(aggregated_processed)

    if verbose:
        def _sample(s: Set[str], n=10):
            return list(sorted(s))[:n]
        print(f"[reconcile] processed-to-skip count: {len(skip_processed)} sample: {_sample(skip_processed)}")
        for r in list(sorted(accounted_pii_raw))[:5]:
            print(f"  [map] PII raw {r} → proc {sorted(raw_to_proc.get(r, set()))}")
        for r in list(sorted(accounted_dates_raw))[:5]:
            print(f"  [map] DATES raw {r} → proc {sorted(raw_to_proc.get(r, set()))}")

    # Filter leaves: exclude IDs / tcode
    skip_proc = {"R0_StudyID", "R0_TCode"}
    skip_raw = {"StudyID", "TCode"}

    results: Dict[str, dict] = {}
    mismatched: List[str] = []
    perfect: List[str] = []

    # Main loop
    for r0_leaf in sorted(proc for proc in processed_leaves if proc not in skip_proc):
        if r0_leaf in skip_processed:
            if verbose:
                print(f"[reconcile] SKIP {r0_leaf} due to {proc_skip_reason.get(r0_leaf,'reason-unknown')}")
            continue

        mapped_raws = pairs.get(r0_leaf, [])
        if not mapped_raws:
            continue

        # ORIGINAL (raw) frequencies for all mapped raw columns
        per_raw_original: Dict[str, Dict[Any, int]] = {}
        for raw_name in mapped_raws:
            if raw_name in skip_raw:
                continue
            if raw_name in raw_pivot_df.columns:
                per_raw_original[raw_name] = _value_counts_canon(raw_pivot_df[raw_name])

        if not per_raw_original:
            continue

        sum_original = Counter()
        for cnt in per_raw_original.values():
            sum_original.update(cnt)

        # EXPECTED via change-tracking
        sum_expected = Counter()
        for raw_name, orig in per_raw_original.items():
            ct_map = (change_tracking or {}).get(raw_name, {})
            exp = _apply_change_chain(orig, ct_map)
            sum_expected.update(exp)

        # ACTUAL from processed JSON (instance-aware collector uses schema)
        actual_vals = _collect_values_for_processed_leaf(processed_json, r0_leaf, schema=schema)
        actual_counts = _value_counts_canon(pd.Series(actual_vals, dtype=object))

        expected = {k: int(v) for k, v in sum_expected.items() if v != 0}
        actual = {k: int(v) for k, v in actual_counts.items() if v != 0}

        keys = set(expected) | set(actual)
        discrepancies = {("null" if k is None else str(k)): {"expected": expected.get(k, 0), "actual": actual.get(k, 0)}
                         for k in keys if expected.get(k, 0) != actual.get(k, 0)}

        is_perfect = len(discrepancies) == 0
        if verbose and not is_perfect:
            disc_preview = dict(list(discrepancies.items())[:5])
            print(f"[reconcile] MISMATCH {r0_leaf} → preview {disc_preview}")

        (perfect if is_perfect else mismatched).append(r0_leaf)

        results[r0_leaf] = {
            "original_frequencies": _jsonify_freq(dict(sum_original)),
            "expected_frequencies": _jsonify_freq(expected),
            "actual_frequencies": _jsonify_freq(actual),
            "discrepancies": discrepancies,
            "perfect_match": is_perfect,
        }

    #  BreastCancer-specific refinement for R0_BCDrugRegimenOngoing 
    if section_name == "BreastCancer":
        try:
            # 1) Drop the aggregated BC-level Ongoing leaves if present
            for agg in ["R0_BCDrugRegimenOngoing_1",
                        "R0_BCDrugRegimenOngoing_2",
                        "R0_BCDrugRegimenOngoing_3"]:
                if agg in results:
                    if verbose:
                        print(f"[reconcile][BC Ongoing] Removing aggregated {agg}")
                    results.pop(agg, None)
                    if agg in mismatched:
                        mismatched.remove(agg)
                    if agg in perfect:
                        perfect.remove(agg)

            # 2) Add per-episode BC-specific reconciliation (7 vars total)
            bc_extra = _reconcile_breast_cancer_drug_ongoing(
                raw_pivot_df=raw_pivot_df,
                processed_json=processed_json,
                resolver_index=resolver_index,
                verbose=verbose,
            )

            for name, payload in bc_extra.items():
                results[name] = payload
                if payload.get("perfect_match"):
                    perfect.append(name)
                else:
                    mismatched.append(name)

        except Exception as e:
            if verbose:
                print(f"[reconcile][BC Ongoing] refinement failed: {e}")


    stage_name = f"{section_name}_ValueReconciliation"

    stage = {
        "variables_checked": len(results),
        "variables_with_perfect_match": len(perfect),
        "variables_with_mismatches": len(mismatched),
        "mismatched_variables": sorted(mismatched),
        "perfect_match_variables": sorted(perfect),
        "status": "PASSED" if not mismatched else "WARNING",
        "reconciliation_details": results,
    }

    if verbose:
        print(f"[reconcile] mismatched BEFORE prune: {len(mismatched)} → {sorted(mismatched)[:15]}")

    # Remove any skipped variables from tallies (safety)
    if skip_processed:
        for k in list(results.keys()):
            if k in skip_processed:
                if verbose:
                    print(f"[reconcile] PRUNE skipped {k} from results/tallies")
                results.pop(k, None)
                if k in mismatched:
                    mismatched.remove(k)
                if k in perfect:
                    perfect.remove(k)
        stage = {
            "variables_checked": len(results),
            "variables_with_perfect_match": len(perfect),
            "variables_with_mismatches": len(mismatched),
            "mismatched_variables": sorted(mismatched),
            "perfect_match_variables": sorted(perfect),
            "status": "PASSED" if not mismatched else "WARNING",
            "reconciliation_details": results,
        }

    if verbose:
        print(f"[reconcile] mismatched AFTER prune: {len(stage['mismatched_variables'])} → {stage['mismatched_variables'][:15]}")

    if save_to:
        out_dir = _ensure_validation_dir(save_to)
        if out_dir:
            out_path = os.path.join(out_dir, "value_reconciliation.json")
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump({stage_name: stage}, f, ensure_ascii=False, indent=2)
            stage["written_to"] = out_path

    return {stage_name: stage}