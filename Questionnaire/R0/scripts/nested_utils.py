"""
Variable-renaming and section-specific resolution utilities.

This module is responsible for mapping raw questionnaire variable names
(coming directly from the database) to their corresponding JSON Schema
fields and array positions.

It provides:
- Section slug helpers and regular expressions to recognise different
  questionnaire sections (Pregnancies, AlcoholSmokingDiet, XRays, etc.).
- A registry that indexes schema leaf fields by tokens, enabling fuzzy
  matching of raw variable names.
- `rename_variable`, which returns detailed information about where a
  raw variable should live in the nested JSON (top-level field, array
  item, index field, etc.).

Other modules (e.g. `common_utils`, `restructure_utils`, `pseudo_anon_utils`,
`qc_utils`) rely on this logic to consistently interpret raw variable names.
"""

from __future__ import annotations
import json, os, re
from typing import Dict, List, Optional, Tuple
import pandas as pd

# Globals
_VARFLAG: Dict[str, str] = {}
_SCHEMA_INDEX: Dict[str, dict] = {}
_TOKEN_TO_LEAF: Dict[str, dict] = {}
_RAWVAR_META: Dict[str, dict] = {}

_XRAY_META: Optional[dict] = None
_ARRAYS_WITH_EXTRA: Dict[Tuple[str, str], dict] = {}

_SECTION_SLUGS = {
    "GeneralInformation": "general_information",
    "PhysicalDevelopment": "physical_dev",
    "Pregnancies": "pregnancies",
    "XRays": "xrays",
    "BreastCancer": "breast_cancer",
    "BreastDisease": "breast_disease",
    "MenstrualMenopause": "menstrual_menopause",
    "AlcoholSmokingDiet": "alcohol_smoking_diet",
    "Jobs": "jobs",
    "PhysicalActivity": "physical_activity",
    "ContraceptiveHRT": "contraceptive_hrt",
    "CancerRelatives": "cancer_relatives",
    "MH_Illnesses": "mh_illnesses",
    "BirthDetails": "birth_details",
    "Mammograms": "mammograms",
    "MH_CancersBenignTumors": "cancers_benign_tumors",
    "MH_DrugsSupplements": "drugs_supplements",
    "OtherBreastSurgery": "other_breast_surgery",
    "OtherLifestyleFactors": "other_lifestyle_factors"
}

_ALPHA_MAP = {c: i+1 for i, c in enumerate("ABCDEFGHIJKLMNOPQRSTUVWXYZ")}

def _norm_section(s: str) -> str:
    """
    Normalise a raw section label (from Questions table) into a canonical form.

    This helps downstream code reliably determine which section-specific
    rules to apply, even if there are small spelling/capitalisation variations due to human error.
    """
    return (s or "").replace("_", "").lower()



"""
Return True if the slug corresponds to a section.

This is used to route variables to the correct alternative resolver logic.
"""
def _is_preg_section(s: str) -> bool: return _norm_section(s) in ("pregnancies", "pregnancy")
def _is_asd_section(s: str) -> bool:  return _norm_section(s) in ("alcoholsmokingdiet", "asd")
def _is_xray_section(s: str) -> bool: return _norm_section(s) in ("xrays", "xray")
def _is_mm_section(s: str) -> bool: return _norm_section(s) in ("menstrualmenopause", "menstrual_menopause")
def _is_pa_section(s: str) -> bool: return _norm_section(s) in ("physicalactivity", "physical_activity")
def _is_pd_section(s: str) -> bool: return _norm_section(s) in ("physicaldevelopment", "physicaldev")
def _is_olf_section(s: str) -> bool: return _norm_section(s) in ("other_lifestyle_factors")

# General setup helpers
def load_schemas_from_paths(paths: dict) -> Dict[str, dict]:
    out = {}
    for sec, p in paths.items():
        try:
            if not os.path.exists(p): continue
            with open(p, "r", encoding="utf-8") as f:
                sch = json.load(f)
            slug = _SECTION_SLUGS.get(sec, sec.lower())
            out[slug] = sch
        except Exception:
            pass
    return out

# def load_varflag_from_excel(path: str, var_col="VariableName", desc_col="VarDesc") -> Dict[str, str]:
#     if path.lower().endswith((".xlsx", ".xls")):
#         df = pd.read_excel(path)
#     else:
#         df = pd.read_csv(path)
#     candidates_var = [var_col, "VarName", "Variable", "Variable_Name"]
#     candidates_desc = [desc_col, "Var_Desc", "Var Description", "VarDesciption", "Desc"]
#     vcol = next((c for c in candidates_var if c in df.columns), None)
#     dcol = next((c for c in candidates_desc if c in df.columns), None)
#     if not vcol or not dcol:
#         raise ValueError(f"Could not find variable/desc columns in {path}. Columns = {list(df.columns)}")
#     m = {}
#     for _, r in df[[vcol, dcol]].dropna(subset=[vcol]).iterrows():
#         rv = str(r[vcol]).strip()
#         vd = str(r[dcol]).strip() if pd.notna(r[dcol]) else ""
#         if rv and vd: m[rv] = vd
#     return m

def init_dynamic_registry(varflag: Optional[Dict[str, str]] = None,
                          schemas_by_slug: Optional[Dict[str, dict]] = None) -> None:
    """
    Build and cache a registry of schema leaf fields for all sections.

    The registry:
    - Indexes schema leaf fields by tokenised names for fast fuzzy lookup.
    - Stores VarFlagging information so we can ignore PII-only variables
      when mapping.
    - Is later used by `rename_variable` to resolve unknown variable names
      on the fly.

    Parameters
    ----------
    varflag_df : pd.DataFrame
        VarFlagging metadata (including PII flags).
    schemas_by_slug : dict
        {section_slug: schema_dict} for all questionnaire sections.
    """
    global _VARFLAG, _SCHEMA_INDEX, _TOKEN_TO_LEAF, _RAWVAR_META, _XRAY_META, _ARRAYS_WITH_EXTRA

    # VarFlag (prefer caller-provided; otherwise optional default file)
    if varflag is None:
        _VARFLAG = {}
    else:
        _VARFLAG = dict(varflag)

    # Schemas (prefer caller-provided; otherwise try defaults)
    if schemas_by_slug is None:
        guesses = {
            "GeneralInformation": "/mnt/data/GeneralInformation_Schema.json",
            "PhysicalDevelopment": "/mnt/data/PhysicalDevelopment_Schema.json",
            "Pregnancies": "/mnt/data/Pregnancies_Schema.json",
            "XRays": "/mnt/data/XRays_Schema.json",
            "BreastCancer": "/mnt/data/BreastCancer_Schema.json",
            "MenstrualMenopause": "/mnt/data/MenstrualMenopause_Schema.json",
            "AlcoholSmokingDiet": "/mnt/data/AlcoholSmokingDiet_Schema.json",
            "BreastDisease": "/mnt/data/BreastDisease_Schema.json",
            "Jobs": "/mnt/data/Jobs_Schema.json",
            "ContraceptiveHRT": "/mnt/data/ContraceptiveHRT_Schema.json",
            "CancerRelatives": "/mnt/data/CancerRelatives_Schema.json",
            "MH_Illnesses": "/mnt/data/MH_Illnesses_Schema.json",
            "Birth_Details": "/mnt/data/BirthDetails_Schema.json",
            "PhysicalActivity": "/mnt/data/PhysicalActivity_Schema.json"
        }
        _SCHEMA_INDEX = load_schemas_from_paths(guesses)
    else:
        _SCHEMA_INDEX = dict(schemas_by_slug)

    # Build token→leaf index (normalized, no 'R0_')
    _TOKEN_TO_LEAF = _index_schema_tokens(_SCHEMA_INDEX)

    # XRays meta (new schema: 12 parent items, child extras)
    _XRAY_META = _build_xray_meta(_SCHEMA_INDEX)

    # Build generic "arrays with Extra" map (Menstrual arrays, XRays, etc.)
    _ARRAYS_WITH_EXTRA = _build_arrays_with_extra(_SCHEMA_INDEX)

    # Build registry that accepts raw, VarDesc, and R0_*# keys
    _RAWVAR_META = _materialize_registry_accepting_all_keys(_VARFLAG, _TOKEN_TO_LEAF, _SCHEMA_INDEX)

# ------------------------------
# Public resolver
# ------------------------------

def rename_variable(key: str) -> Optional[dict]:
    """
    Resolve a raw variable name into schema and array metadata.

    This is the central resolver used throughout the ETL.

    It returns a dict describing:
    - `section`: which section this variable belongs to.
    - `schema_field`: the corresponding JSON field name.
    - `array_path`: list of array names to reach the field (if nested).
    - `indices` / `index_label`: indices or band labels for arrays
      (e.g. pregnancy number, weight record number, age bands).
    - any other section-specific hints needed to place the value.

    For complex sections (Pregnancies, XRays, PhysicalDevelopment, etc.),
    the function applies bespoke heuristics and regex patterns to decode
    encodings (letter bands, number suffixes, etc.).
    """
    if not _RAWVAR_META:
        init_dynamic_registry()

    k = str(key).strip()
    meta = _RAWVAR_META.get(k)
    if meta:
        return meta

    # 1) Try dynamic resolution against this key
    meta = _resolve_on_the_fly(k, _TOKEN_TO_LEAF, _SCHEMA_INDEX)
    if meta:
        meta = _xray_retarget_meta(meta, k)          # XRays: keep inst 1..12 in parent; extras attach inside
        meta = _generic_extra_retarget_meta(meta, k) # Menstrual (and other sections with ...Extra)
        _RAWVAR_META[k] = meta
        return meta

    # 2) Try via VarDesc mapping (raw -> VarDesc)
    vdesc = _VARFLAG.get(k)
    if vdesc:
        meta = _resolve_on_the_fly(vdesc, _TOKEN_TO_LEAF, _SCHEMA_INDEX)
        if meta:
            meta = _xray_retarget_meta(meta, f"{vdesc} {k}")
            meta = _generic_extra_retarget_meta(meta, f"{vdesc} {k}")
            _RAWVAR_META[k] = meta
            _RAWVAR_META[str(vdesc).strip()] = meta
            return meta

    return None

# Internals (tokenization & matching)

def _normalize_token(s: str) -> str:
    s = s.strip()
    if s.lower().startswith("r0_"): s = s[3:]
    s = re.sub(r'[^A-Za-z0-9]', '', s)
    return s.lower()

def _tokenize_words(s: str) -> List[str]:
    return re.findall(r'[A-Za-z]+', str(s or '').lower())

# canonicalization: normalize synonyms so VarDesc ↔ schema names align
_SEMANTIC_ALIASES = [
    # pregnancies
    (re.compile(r'pregnancy', re.I), 'preg'),
    (re.compile(r'months?', re.I), 'mnth'),
    (re.compile(r'weeks?', re.I), 'wks'),
    (re.compile(r'years?', re.I), 'yr'),
    # weights etc.
    (re.compile(r'weight', re.I), 'wght'),
    (re.compile(r'pounds?', re.I), 'lbs'),
    (re.compile(r'ounces?', re.I), 'ozs'),
    # breastfeeding/duration
    (re.compile(r'breastfeedingweeks', re.I), 'breastfeedingwks'),
    (re.compile(r'durationweeks', re.I), 'durationwks'),
    # endings
    (re.compile(r'endmonth', re.I), 'endmnth'),
    (re.compile(r'\bend\s*year\b', re.I), 'endyr'),
    # ASD
    (re.compile(r'agegroup|ageband', re.I), 'age'),
    # Menstrual/Menopause: periods stopped ↔ tempstop*
    (re.compile(r'periods?\s*stop(?:ped)?', re.I), 'tempstop'),
    (re.compile(r'temporar(?:ily)?\s*stop(?:ped)?', re.I), 'tempstop'),
    (re.compile(r'tempstopped', re.I), 'tempstop'),
    # Ovary/Uterus/Operations tokens
    (re.compile(r'ovaries|ovary', re.I), 'ovary'),
    (re.compile(r'uterus', re.I), 'uterus'),
    (re.compile(r'operation|surgery|op', re.I), 'op'),
    # from/to age/month/year
    (re.compile(r'from\s*age', re.I), 'fromage'),
    (re.compile(r'to\s*age', re.I), 'toage'),
    (re.compile(r'from\s*month', re.I), 'frommnth'),
    (re.compile(r'to\s*month', re.I), 'tomnth'),
    (re.compile(r'from\s*year', re.I), 'fromyr'),
    (re.compile(r'to\s*year', re.I), 'toyr'),

    (re.compile(r'^gen\d+_full@', re.I), ''),

    # map Q10_2* tokens to schema-ish names
    (re.compile(r'\bq\d+_2typ\b',   re.I), 'nightworktype'),
    (re.compile(r'\bq\d+_2start\b', re.I), 'nightworkstartyr'),
    (re.compile(r'\bq\d+_2end\b',   re.I), 'nightworkendyr'),
    (re.compile(r'\bq\d+_2days\b',  re.I), 'nightworkdaysweek'),
    (re.compile(r'\bq\d+_2hrs\b',   re.I), 'nightworkhrsweek'),
]

def _apply_semantic_aliases(s: str) -> str:
    out = s
    for rx, repl in _SEMANTIC_ALIASES: out = rx.sub(repl, out)
    return out

def _canonicalize_for_match(s: str) -> str:
    base = _normalize_token(_apply_semantic_aliases(s))
    return base

def _index_schema_tokens(schemas_by_slug: Dict[str, dict]) -> Dict[str, dict]:
    token_map: Dict[str, dict] = {}
    def walk(node: dict, slug: str, path: List[str]) -> None:
        if not isinstance(node, dict): return
        props = node.get("properties")
        if isinstance(props, dict):
            for k, v in props.items():
                if isinstance(v, dict):
                    if v.get("type") == "array" and "items" in v:
                        walk(v.get("items", {}), slug, path + [k])
                    elif "properties" in v:
                        walk(v, slug, path)
                    else:
                        _register_leaf(token_map, slug, k, path)
                else:
                    _register_leaf(token_map, slug, k, path)
        if node.get("type") == "array" and "items" in node:
            walk(node["items"], slug, path)
    for slug, schema in schemas_by_slug.items():
        if isinstance(schema, dict):
            walk(schema, slug, [])
    return token_map

def _register_leaf(token_map: Dict[str, dict], slug: str, schema_field_key: str, array_path: List[str]) -> None:
    token = schema_field_key[3:] if schema_field_key.startswith("R0_") else schema_field_key
    info = {
        "section": slug,
        "schema_field": schema_field_key,
        "array_path": list(array_path),
        "array_name": array_path[-1] if array_path else None,
    }
    token_map.setdefault(_normalize_token(token), info)
    token_map.setdefault(_canonicalize_for_match(token), info)

def _split_descriptor(s: str) -> Tuple[str, List[int]]:
    s = str(s or "").strip()
    if not s: return "", []
    s_norm = re.sub(r'(?<=_)([A-Z])(?=[_\W]|$)', lambda m: str(_ALPHA_MAP.get(m.group(1), m.group(1))), s)
    base = re.sub(r'\d+', '', s_norm)
    base = re.sub(r'__+', '_', base).strip('_')
    nums = list(map(int, re.findall(r'(\d+)', s_norm)))
    return base, nums

def _array_max_items_list(leaf_info: dict, schemas_by_slug: Dict[str, dict]) -> List[Optional[int]]:
    slug = leaf_info["section"]
    schema = schemas_by_slug.get(slug, {})
    out = []
    node = schema
    for seg in leaf_info["array_path"]:
        props = node.get("properties", {}) if isinstance(node, dict) else {}
        arr = props.get(seg, {})
        out.append(arr.get("maxItems"))
        node = arr.get("items", {})
    return out

def _pick_indices_from_text(text: str, needed: int, max_items_list: List[Optional[int]]) -> List[int]:
    nums = list(map(int, re.findall(r'(\d+)', str(text))))
    chosen = []
    for n in reversed(nums):
        if len(chosen) >= needed: break
        idx_pos = len(chosen)
        maxi = max_items_list[idx_pos] if idx_pos < len(max_items_list) else None
        if maxi is None:
            if 1 <= n <= 100: chosen.append(n)
        else:
            if 1 <= n <= int(maxi): chosen.append(n)
    chosen.reverse()
    while len(chosen) < needed: chosen.append(1)
    return chosen

# Menstrual/Menopause (MM) Cur/20/40 bands
# Applies to arrays: CycleDays, BreastDiscomfort, IrregularCycles, FlowDays

MM_ARRAYS = {"CycleDays", "BreastDiscomfort", "IrregularCycles", "FlowDays"}
MM_BAND_ORDER = {"cur": 1, "20": 2, "40": 3}
MM_TEXT_RE = re.compile(r'(now|current|around\s*age\s*20|age\s*20|around\s*20|around\s*age\s*40|age\s*40|around\s*40)', re.I)
MM_SUFFIX_RE = re.compile(r'_(Cur|20|40)(?:\b|_)', re.I)

def _extract_mm_band(desc_or_raw: str):
    s = str(desc_or_raw or "")
    # explicit suffix like _Cur, _20, _40
    m = MM_SUFFIX_RE.search(s)
    if m:
        code = m.group(1).lower()
        code = "cur" if code == "cur" else code
        if code in MM_BAND_ORDER:
            return code
    # textual clues in descriptions
    m = MM_TEXT_RE.search(s)
    if m:
        t = m.group(1).lower()
        if "now" in t or "current" in t:
            return "cur"
        if "20" in t:
            return "20"
        if "40" in t:
            return "40"
    return None

def _mm_label(key: str) -> str:
    # normalize internal key to label stored in *_Num
    return "Cur" if key == "cur" else key

# ---- Pregnancies & ASD helpers (indices/age-bands)
_PREG_RE = re.compile(r'preg\w*?(\d+)', flags=re.IGNORECASE)
def _pregnancy_index_from_desc(base_token: str) -> Optional[int]:
    s = base_token or ""
    m = _PREG_RE.search(s)
    if m:
        try: return int(m.group(1))
        except Exception: return None
    return None

AGE_UNDER_RE = re.compile(r'under\s*(\d+)', re.I)
AGE_OVER_RE  = re.compile(r'(?:over\s*(\d+)|(\d+)\s*\+|(\d+)\s*plus)', re.I)
AGE_RANGE_RE = re.compile(r'(?:age\s*)?(\d{1,2})\s*(?:-|–|to|_)\s*(\d{1,2})', re.I)
ASD_LETTER_RE  = re.compile(r'_(A|B|C)_', re.I)

# Physical Development (PD) RecordedWeights Cur/20/40/60 bands
# Applies to array: RecordedWeights, RecordedHeights
PD_DEBUG = False

def _pd_dbg(*args):
    if PD_DEBUG:
        try:
            print("[PD-DEBUG]", *args)
        except Exception:
            pass

PD_ARRAY = "RecordedWeights"
PD_BAND_ORDER = {"cur": 1, "20": 2, "40": 3, "60": 4}
PD_NUM_FIELD = "R0_RecWght_Num"

# textual clues in descriptions
PD_TEXT_RE = re.compile(
    r'(now|current|age\s*20|around\s*age\s*20|age\s*40|around\s*age\s*40|age\s*60|around\s*age\s*60)',
    re.I
)

# explicit suffix like _Cur, _20, _40, _60
PD_SUFFIX_RE = re.compile(r'_(Cur|20|40|60)(?:\b|_)', re.I)

# raw-code clues (from your mapping): Q3_8 → Cur; Q3_3 → 20; Q3_4 → 40; Q3_5 → 60
PD_Q_RE = re.compile(r'\bQ3_(\d+)\b', re.I)

def _extract_pd_weight_band(desc_or_raw: str) -> str | None:
    s = str(desc_or_raw or "")
    _pd_dbg("WEIGHT.extract start", {"text": s})

    m = PD_SUFFIX_RE.search(s)
    if m:
        code = m.group(1).lower()
        res = "cur" if code == "cur" else code
        _pd_dbg("WEIGHT.extract via SUFFIX", {"match": m.group(0), "res": res})
        return res

    if re.search(r'R0_WghtAge_(?:Kg|Lbs|St)1\b', s, flags=re.I):
        _pd_dbg("WEIGHT.extract via TRAILING_1 current")
        return "cur"

    m = PD_Q_RE.search(s)
    if m:
        qn = m.group(1)
        mp = {"8": "cur", "3": "20", "4": "40", "5": "60"}
        if qn in mp:
            _pd_dbg("WEIGHT.extract via QID", {"qid": qn, "res": mp[qn]})
            return mp[qn]

    m = PD_TEXT_RE.search(s)
    if m:
        t = m.group(1).lower()
        res = "cur" if ("current" in t or "now" in t) else ("20" if "20" in t else "40" if "40" in t else "60" if "60" in t else None)
        _pd_dbg("WEIGHT.extract via TEXT", {"token": t, "res": res})
        if res: return res

    m = re.search(r'R0_WghtAge_(?:Kg|Lbs|St)(20|40|60)\b', s, flags=re.I)
    if m:
        _pd_dbg("WEIGHT.extract via NUMERIC_SUFFIX", {"age": m.group(1)})
        return m.group(1)

    _pd_dbg("WEIGHT.extract NONE")
    return None

def _pd_label(key: str) -> str:
    return "Cur" if key == "cur" else key

HD_ARRAY = "RecordedHeights"
HD_BAND_ORDER = {"cur": 1, "20": 2}
HD_NUM_FIELD = "R0_RecHght_Num"

# textual clues in descriptions
HD_TEXT_RE = re.compile(
    r'(now|current|age\s*20|around\s*age\s*20)',
    re.I
)
# explicit suffix like _Cur, _20
HD_SUFFIX_RE = re.compile(r'_(Cur|20)(?:\b|_)', re.I)

def _extract_pd_height_band(desc_or_raw: str) -> str | None:
    s = str(desc_or_raw or "")
    _pd_dbg("HEIGHT.extract start", {"text": s})

    m = HD_SUFFIX_RE.search(s)
    if m:
        code = m.group(1).lower()
        res = "cur" if code == "cur" else code
        _pd_dbg("HEIGHT.extract via SUFFIX", {"match": m.group(0), "res": res})
        return res

    m = HD_TEXT_RE.search(s)
    if m:
        t = m.group(1).lower()
        res = "cur" if ("current" in t or "now" in t) else ("20" if "20" in t else None)
        _pd_dbg("HEIGHT.extract via TEXT", {"token": t, "res": res})
        if res: return res

    m = re.search(r'R0_HghtAge_(?:Ft|In|Cm)(20)\b', s, flags=re.I)
    if m:
        _pd_dbg("HEIGHT.extract via NUMERIC_SUFFIX", {"age": m.group(1)})
        return m.group(1)

    _pd_dbg("HEIGHT.extract NONE")
    return None

def _hd_label(key: str) -> str:
    return "Cur" if key == "cur" else key

# Physical Development (PD) BraSize Cur/20 bands
# Applies to array: BraSize

BRA_ARRAY = "BraSize"
BRA_BAND_ORDER = {"cur": 1, "20": 2}
BRA_NUM_FIELD = "R0_BraSize_Num"

BRA_TEXT_RE = re.compile(
    r'(now|current|age\s*20|around\s*age\s*20)',
    re.I
)
BRA_SUFFIX_RE = re.compile(r'_(Cur|20)(?=$|_|[A-Z])', re.I)

def _extract_pd_bra_band(desc_or_raw: str) -> str | None:
    s = str(desc_or_raw or "")
    _pd_dbg("BRA.extract start", {"text": s})

    m = BRA_SUFFIX_RE.search(s)
    if m:
        code = m.group(1).lower()
        res = "cur" if code == "cur" else code
        _pd_dbg("BRA.extract via SUFFIX", {"match": m.group(0), "res": res})
        return res

    m = BRA_TEXT_RE.search(s)
    if m:
        t = m.group(1).lower()
        res = "cur" if ("current" in t or "now" in t) else ("20" if "20" in t else None)
        _pd_dbg("BRA.extract via TEXT", {"token": t, "res": res})
        if res: return res

    _pd_dbg("BRA.extract NONE")
    # 3) default: base names without an age suffix are "current"
    #    e.g., "BraCupSize" or "BraCupSize_Other" => cur
    if re.search(r'^Bra(?:Cup|Band)Size(?:$|_)(?!20)', s, flags=re.I) and "_20" not in s:
        _pd_dbg("BRA.extract via BASE_DEFAULT", {"res": "cur"})
        return "cur"

    return None

    return None

def _bra_label(key: str) -> str:
    return "Cur" if key == "cur" else key


# Physical Activity (PA) age bands for StrenuousExercise

PA_BANDS = {
    "18_29": (18, 29),
    "30_49": (30, 49),
    "50plus": (50, 10**6)
}
PA_NUM_TO_KEY = {"1": "18_29", "2": "30_49", "3": "50plus"}
PA_KEY_RE   = re.compile(r'(18[_-]?29|30[_-]?49|50\s*plus)', re.I)
PA_NUM_RE   = re.compile(r'_(1|2|3)_') 

def _extract_pa_age_band(desc_or_raw: str):
    s = str(desc_or_raw or "")
    # textual key in VarDesc (e.g., "StrenuousExerciseDays18_29", "…30_49", "…50Plus")
    m = PA_KEY_RE.search(s)
    if m:
        key = m.group(1).lower().replace("-", "_").replace(" ", "")
        key = "50plus" if "50" in key and "plus" in key else key
        return (key, *PA_BANDS[key])
    m = PA_NUM_RE.search(s)
    if m:
        num = m.group(1)
        key = PA_NUM_TO_KEY.get(num)
        if key:
            a, b = PA_BANDS[key]
            return (key, a, b)
    return None

def _ageband_label_pa(key: str, start: int, end: int) -> str:
    if key.endswith("plus"): return f"{start}+"
    if "_" in key:
        a, b = key.split("_", 1); return f"{a}-{b}"
    return key

def _compute_pa_ageband_orders(varflag_map: Dict[str, str], token_index: Dict[str, dict], schemas_by_slug: Dict[str, dict]):
    orders, bands_by_path = {}, {}
    for raw_var, desc in (varflag_map or {}).items():
        base, _ = _split_descriptor(desc)
        leaf = _token_match(base, token_index)
        if not leaf or not _is_pa_section(leaf["section"]):
            continue
        apath = tuple(leaf["array_path"])
        if not apath:
            continue
        band = _extract_pa_age_band(desc) or _extract_pa_age_band(raw_var)
        if not band:
            continue
        key, start, end = band
        bands_by_path.setdefault((leaf["section"], apath), {})[key] = (start, end)
    for path_key, d in bands_by_path.items():
        sorted_keys = sorted(d.items(), key=lambda kv: (kv[1][0], kv[1][1], kv[0]))
        orders[path_key] = { key: i+1 for i, (key, _) in enumerate(sorted_keys) }
    return orders


# Night wakeups (OtherLifestyleFactors → NightWakeups)
# Instances: 1..2 → "Rec", "20"
NW_ARRAY = "NightWakeups"
NW_NUM_FIELD = "R0_NightWakeup_Num"

# Schema-defined labels in display order (instances 1..2)
# OtherLifestyleFactors_Schema.json → enum: ["Rec", "20"]
NW_LABELS = {1: "Rec", 2: "20"}

def _nw_label(idx: int) -> Optional[str]:
    try:
        return NW_LABELS.get(int(idx))
    except Exception:
        return None


ASD_LETTER_MAP = {'A': ('18_24', 18, 24),'B': ('25_49', 25, 49),'C': ('50plus', 50, 10**6)}

def _extract_age_band(desc_or_raw: str) -> Optional[Tuple[str, int, int]]:
    s = str(desc_or_raw or ""); s_compact = s.replace(" ", "")
    m = AGE_UNDER_RE.search(s_compact)
    if m: n = int(m.group(1));  return (f"under{n}", -1, n)
    m = AGE_OVER_RE.search(s_compact)
    if m: n = next(int(g) for g in m.groups() if g);  return (f"{n}plus", n, 10**6)
    m = AGE_RANGE_RE.search(s_compact)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        if a > b: a, b = b, a
        return (f"{a}_{b}", a, b)
    return None

def _extract_asd_letter_band(raw_key: str) -> Optional[Tuple[str, int, int]]:
    m = ASD_LETTER_RE.search(str(raw_key or ""))
    if not m: return None
    code = m.group(1).upper()
    if code in ASD_LETTER_MAP:
        key, start, end = ASD_LETTER_MAP[code]
        return (key, start, end)
    return None

def _ageband_label(key: str, start: int, end: int) -> str:
    if key.startswith("under"): return f"Under{end}"
    if key.endswith("plus"):    return f"{start}+"
    if "_" in key:
        a, b = key.split("_", 1); return f"{a}-{b}"
    return key

def _compute_asd_ageband_orders(varflag_map: Dict[str, str], token_index: Dict[str, dict], schemas_by_slug: Dict[str, dict]):
    orders, bands_by_path = {}, {}
    for raw_var, desc in (varflag_map or {}).items():
        base, _ = _split_descriptor(desc)
        leaf = _token_match(base, token_index)
        if not leaf or not _is_asd_section(leaf["section"]): continue
        apath = tuple(leaf["array_path"])
        if not apath: continue
        band = _extract_age_band(desc) or _extract_age_band(raw_var) or _extract_asd_letter_band(raw_var)
        if not band: continue
        key, start, end = band
        bands_by_path.setdefault((leaf["section"], apath), {})[key] = (start, end)
    for path_key, d in bands_by_path.items():
        sorted_keys = sorted(d.items(), key=lambda kv: (kv[1][0], kv[1][1], kv[0]))
        orders[path_key] = { key: i+1 for i, (key, _) in enumerate(sorted_keys) }
    return orders

def _compose_indices(v_nums: List[int], leaf_info: dict, source_text: str, schemas_by_slug: Dict[str, dict],
                     asd_orders=None, base_token: Optional[str]=None):
    depth = len(leaf_info["array_path"])
    if depth == 0: return [], None
    nums = list(v_nums[:depth]); label = None

    # pregnancies
    if depth >= 1 and _is_preg_section(leaf_info.get("section")):
        preg_nr = v_nums[0] if v_nums else _pregnancy_index_from_desc(source_text if base_token is None else base_token)
        if preg_nr is not None:
            if nums: nums[0] = preg_nr
            else:    nums.append(preg_nr)

    # ASD (age-band → ordered index + label)
    if depth >= 1 and _is_asd_section(leaf_info.get("section")):
        path_key = (leaf_info["section"], tuple(leaf_info["array_path"]))
        band = _extract_age_band(source_text if base_token is None else base_token) or _extract_asd_letter_band(source_text)
        if band:
            band_key, start, end = band
            label = _ageband_label(band_key, start, end)
            ord_idx = None
            if asd_orders:
                ord_idx = asd_orders.get(path_key, {}).get(band_key)
            if ord_idx is None:
                order_hint = [(-1,16), (16,24), (18,24), (25,49), (50,10**6)]
                try: ord_idx = 1 + next(i for i,(a,b) in enumerate(order_hint) if a==start and b==end)
                except StopIteration: ord_idx = 1
            if nums: nums[0] = ord_idx
            else:    nums.append(ord_idx)
    # PA (age-band → ordered index + label)
    if depth >= 1 and _is_pa_section(leaf_info.get("section")):
        path_key = (leaf_info["section"], tuple(leaf_info["array_path"]))
        band = _extract_pa_age_band(source_text if base_token is None else base_token) or _extract_pa_age_band(source_text)
        if band:
            band_key, start, end = band
            label = _ageband_label_pa(band_key, start, end)
            ord_idx = None
            if asd_orders:
                ord_idx = asd_orders.get(path_key, {}).get(band_key)
            if ord_idx is None:
                # fallback: order by PA_BANDS start/end
                try:
                    order_hint = sorted([(a,b,k) for k,(a,b) in PA_BANDS.items()])
                    ord_idx = 1 + next(i for i,(a,b,k) in enumerate(order_hint) if k==band_key)
                except StopIteration:
                    ord_idx = 1
            if nums: nums[0] = ord_idx
            else:    nums.append(ord_idx)

    # MM Cur/20/40 (→ ordered index + label)
    if depth >= 1 and _is_mm_section(leaf_info.get("section")):
        # Only apply to our specific arrays
        apath = list(leaf_info.get("array_path") or [])
        if apath and (apath[0] in MM_ARRAYS):
            key = _extract_mm_band(source_text if base_token is None else base_token) or _extract_mm_band(source_text)
            if key:
                label = _mm_label(key)
                ord_idx = MM_BAND_ORDER.get(key, 1)
                if nums: 
                    nums[0] = ord_idx
                else:    
                    nums.append(ord_idx)

    
    # PD RecordedWeights ...
    if depth >= 1 and _is_pd_section(leaf_info.get("section")):
        apath = list(leaf_info.get("array_path") or [])
        _pd_dbg("WEIGHT.compose check", {"apath": apath, "nums_in": nums, "source": source_text, "base": base_token})
        if apath and apath[0] == PD_ARRAY:
            key = _extract_pd_weight_band(source_text if base_token is None else base_token) or \
                  _extract_pd_weight_band(source_text)
            _pd_dbg("WEIGHT.compose extracted", {"key": key})
            if key:
                label = _pd_label(key)
                ord_idx = PD_BAND_ORDER.get(key, 1)
                _pd_dbg("WEIGHT.compose apply", {"ord": ord_idx, "label": label, "nums_before": nums})
                if nums:
                    nums[0] = ord_idx
                else:
                    nums.append(ord_idx)
                _pd_dbg("WEIGHT.compose nums_after", {"nums_after": nums})


    # PD RecordedHeights ...
    if depth >= 1 and _is_pd_section(leaf_info.get("section")):
        apath = list(leaf_info.get("array_path") or [])
        _pd_dbg("HEIGHT.compose check", {"apath": apath, "nums_in": nums, "source": source_text, "base": base_token})
        if apath and apath[0] == HD_ARRAY:
            key = _extract_pd_height_band(source_text if base_token is None else base_token) or \
                  _extract_pd_height_band(source_text)
            _pd_dbg("HEIGHT.compose extracted", {"key": key})
            if key:
                label = _hd_label(key)
                ord_idx = HD_BAND_ORDER.get(key, 1)
                _pd_dbg("HEIGHT.compose apply", {"ord": ord_idx, "label": label, "nums_before": nums})
                if nums:
                    nums[0] = ord_idx
                else:
                    nums.append(ord_idx)
                _pd_dbg("HEIGHT.compose nums_after", {"nums_after": nums})



    # PD BraSize ...
    if depth >= 1 and _is_pd_section(leaf_info.get("section")):
        apath = list(leaf_info.get("array_path") or [])
        _pd_dbg("BRA.compose check", {"apath": apath, "nums_in": nums, "source": source_text, "base": base_token})
        if apath and apath[0] == BRA_ARRAY:
            key = _extract_pd_bra_band(source_text if base_token is None else base_token) or \
                  _extract_pd_bra_band(source_text)
            _pd_dbg("BRA.compose extracted", {"key": key})
            if key:
                label = _bra_label(key)
                ord_idx = BRA_BAND_ORDER.get(key, 1)
                _pd_dbg("BRA.compose apply", {"ord": ord_idx, "label": label, "nums_before": nums})
                if nums:
                    nums[0] = ord_idx
                else:
                    nums.append(ord_idx)
                _pd_dbg("BRA.compose nums_after", {"nums_after": nums})

    # XRays → ChestXrayEvents (1..4) → label = schema enum ("U20","20-39","40-59","60+")
    if depth >= 1 and _is_xray_section(leaf_info.get("section")):
        apath = list(leaf_info.get("array_path") or [])
        if apath and apath[0] == CX_ARRAY:
            text = (base_token or source_text or "").lower()

            # Explicitly map your four processed/raw variables to the 4 bands
            if "under20" in text or "u20" in text:
                band_idx = 1     # U20
            elif "20_39" in text or "20-39" in text:
                band_idx = 2     # 20–39
            elif "40_59" in text or "40-59" in text:
                band_idx = 3     # 40–59
            elif "60plus" in text or "60+" in text:
                band_idx = 4     # 60+
            else:
                # Fallback: if nums already contains a 1–4 index, use that; otherwise default 1
                if nums and 1 <= nums[0] <= 4:
                    band_idx = int(nums[0])
                else:
                    band_idx = 1

            label = _cx_label(band_idx) or label

            # Make sure nums[0] is the band index 1..4
            if nums:
                nums[0] = band_idx
            else:
                nums.append(band_idx)

    
    # OtherLifestyleFactors → NightWakeups (1..2) → label = schema enum ("Rec","20")
    if depth >= 1 and (leaf_info.get("section") == "other_lifestyle_factors"):
        apath = list(leaf_info.get("array_path") or [])
        if apath and apath[0] == NW_ARRAY:
            # Use provided index (if any) or default to 1
            if nums:
                band_idx = nums[0]
            else:
                band_idx = 1

            try:
                band_idx = int(band_idx)
            except Exception:
                band_idx = 1

            # constrain to 1..2 (NightWakeups maxItems = 2)
            if band_idx < 1:
                band_idx = 1
            elif band_idx > 2:
                band_idx = 2

            label = _nw_label(band_idx) or label

            if nums:
                nums[0] = band_idx
            else:
                nums.append(band_idx)

    if len(nums) < depth:
        max_list = _array_max_items_list(leaf_info, schemas_by_slug)
        inferred = _pick_indices_from_text(source_text, depth - len(nums), max_list)
        nums.extend(inferred)
    if len(nums) < depth: nums.extend([1] * (depth - len(nums)))
    return nums, label


# Strong matching

def _tokenize_words_lower_alias(s: str) -> List[str]:
    return re.findall(r'[A-Za-z]+', _apply_semantic_aliases(s).lower())

def _token_score(base_text: str, leaf_schema_field: str) -> int:
    bt_raw = base_text or ""
    bt_canon = _apply_semantic_aliases(bt_raw).lower()
    bt_compact = _normalize_token(bt_raw)
    bt_words = set(_tokenize_words_lower_alias(bt_raw))

    leaf_raw = leaf_schema_field[3:] if leaf_schema_field.startswith("R0_") else leaf_schema_field
    leaf_canon = _apply_semantic_aliases(leaf_raw).lower()
    leaf_compact = _normalize_token(leaf_raw)
    leaf_words = set(_tokenize_words_lower_alias(leaf_raw))

    score = 0
    if bt_compact == leaf_compact: score += 100
    if bt_canon == leaf_canon:     score += 90
    if bt_compact and (bt_compact in leaf_compact or leaf_compact in bt_compact): score += 40
    if bt_words and leaf_words:    score += 15 * len(bt_words & leaf_words)
    if leaf_compact.startswith(bt_compact) or bt_compact.startswith(leaf_compact): score += 10

    # Menstrual nudges
    if 'tempstop' in bt_canon:
        if 'tempstop' in leaf_canon: score += 50
        if any(w in leaf_canon for w in ('ovary','uterus','op')): score -= 40
    if any(w in bt_canon for w in ('ovary','uterus','op')):
        if any(w in leaf_canon for w in ('ovary','uterus','op')): score += 50
        if 'tempstop' in leaf_canon: score -= 40
    return score

def _best_leaf_match(base_token: str, token_index: Dict[str, dict]) -> Optional[dict]:
    if not base_token: return None
    bt_norm = _normalize_token(base_token)
    bt_canon = _canonicalize_for_match(base_token)
    if bt_norm in token_index:  return token_index[bt_norm]
    if bt_canon in token_index: return token_index[bt_canon]
    best = None; best_score = -10**9
    for info in token_index.values():
        sf = info['schema_field']
        s = _token_score(base_token, sf)
        if s > best_score:
            best_score = s; best = info
    if best_score > 0: return best
    for k, info in token_index.items():
        if bt_norm.startswith(k) or k.startswith(bt_norm) or bt_canon.startswith(k) or k.startswith(bt_canon):
            return info
    return None

def _token_match(base_token: str, token_index: Dict[str, dict]) -> Optional[dict]:
    return _best_leaf_match(base_token, token_index)

# XRays helpers
def _build_xray_meta(schemas_by_slug: Dict[str, dict]) -> Optional[dict]:
    xr = schemas_by_slug.get("xrays")
    if not isinstance(xr, dict): return None
    props = xr.get("properties", {})
    main_arr = props.get("XrayEvents", {})
    if not (isinstance(main_arr, dict) and main_arr.get("type") == "array"): return None
    items = main_arr.get("items", {})
    mprops = (items or {}).get("properties", {}) or {}
    extra_arr = mprops.get("XrayEventsExtra", {})
    eprops = (extra_arr or {}).get("items", {}).get("properties", {}) if isinstance(extra_arr, dict) else {}
    return {
        "section": "xrays",
        "main_array": "XrayEvents",
        "extra_array": "XrayEventsExtra",
        "main_props": set(k for k in mprops.keys() if k != "XrayEventsExtra"),
        "extra_props": set(eprops.keys()),
        "max_main": main_arr.get("maxItems"),
    }

def _extract_xray_instance_from_text(text: str) -> Optional[int]:
    for m in re.findall(r'(?<!\d)(\d{1,2})(?!\d)', str(text) or ""):
        n = int(m)
        if 1 <= n <= 12:
            return n
    return None

def _secondary_index_from_text(source_text: str) -> int:
    nums = [int(x) for x in re.findall(r'(\d+)', str(source_text) or "")]
    if len(nums) >= 2:
        return nums[-1]
    return 1

def _xray_retarget_meta(meta: dict, source_text: str) -> dict:
    """
    Keep ALL instances (1..12) in XrayEvents parent array.
    Extra fields live under XrayEventsExtra inside the same parent.
    """
    if not _XRAY_META or not _is_xray_section(meta.get("section")):
        return meta

    main = _XRAY_META["main_array"]
    extra = _XRAY_META["extra_array"]
    ap = list(meta.get("array_path") or [])
    if not ap or ap[0] != main:
        return meta

    inst = None
    for n in (meta.get("indices") or []):
        try:
            n = int(n)
            if 1 <= n <= 12:
                inst = n; break
        except Exception:
            continue
    if not inst:
        inst = _extract_xray_instance_from_text(source_text)
    if not inst:
        inst = 1

    # Parent-level field
    if ap == [main]:
        return { **meta, "indices": [inst], "entry_num": inst, "array_path": [main], "array_name": main }

    # Extra field under child array — keep it under same parent inst
    if ap == [main, extra]:
        child_idx = _secondary_index_from_text(source_text)
        return { **meta, "indices": [inst, child_idx], "entry_num": child_idx, "array_path": [main, extra], "array_name": extra }

    return meta

# Chest X-ray (XRays) age bands for ChestXrayEvents-
CX_ARRAY = "ChestXrayEvents"
CX_NUM_FIELD = "R0_ChestXray_Num"
# Schema-defined labels in display order (instances 1..4)
CX_LABELS = {1: "U20", 2: "20-39", 3: "40-59", 4: "60+"}  # XRays_Schema.json
def _cx_label(idx: int) -> Optional[str]:
    try:
        return CX_LABELS.get(int(idx))
    except Exception:
        return None


# Generic "*Extra" arrays (Menstrual, XRays, etc.)

def _build_arrays_with_extra(schemas_by_slug: Dict[str, dict]) -> Dict[Tuple[str, str], dict]:
    """
    Build a map for arrays that have a single child array whose key ends with 'Extra'.
    Also tries to parse the child description for hints like 'occasions 2-4' or 'instances 3 and 4'
    to record the (optional) minimum parent instance that typically uses the extra block.
    """
    out: Dict[Tuple[str, str], dict] = {}

    def parse_min_instance(txt: str) -> Optional[int]:
        if not isinstance(txt, str): return None
        # e.g. "occasions 2-4", "instances 3 and 4"
        m = re.search(r'(?:occasions?|instances?)\s*(\d+)(?:\s*(?:-|to|and)\s*\d+)?', txt, flags=re.I)
        if m:
            try: return int(m.group(1))
            except Exception: return None
        return None

    for section_slug, schema in (schemas_by_slug or {}).items():
        if not isinstance(schema, dict): continue
        top_props = (schema.get("properties") or {})
        for arr_name, arr_def in top_props.items():
            if not isinstance(arr_def, dict) or arr_def.get("type") != "array": continue
            items = arr_def.get("items", {})
            item_props = (items.get("properties") or {}) if isinstance(items, dict) else {}
            extra_candidates = {k: v for k, v in item_props.items()
                                if isinstance(v, dict) and v.get("type") == "array" and k.lower().endswith("extra")}
            if not extra_candidates: continue
            extra_name, extra_def = next(iter(extra_candidates.items()))
            extra_props = (extra_def.get("items", {}).get("properties", {}) or {}) if isinstance(extra_def, dict) else {}
            main_props = {k for k in item_props.keys() if k != extra_name}
            min_inst = parse_min_instance(extra_def.get("description", "")) if isinstance(extra_def, dict) else None
            out[(section_slug, arr_name)] = {
                "section": section_slug,
                "main_array": arr_name,
                "extra_array": extra_name,
                "main_props": main_props,
                "extra_props": set(extra_props.keys()),
                "min_instance_for_extra": min_inst,  # optional hint; NOT enforced for explicit child leaves
            }
    return out

def _find_generic_extra_for(meta: dict) -> Optional[dict]:
    if not meta: return None
    section = meta.get("section")
    ap = meta.get("array_path") or []
    if not ap: return None
    key = (section, ap[0])
    return _ARRAYS_WITH_EXTRA.get(key)

def _choose_extra_field_from_text(extra_props: set, source_text: str, fallback_field: Optional[str]) -> Optional[str]:
    text_tok = set(_tokenize_words(_apply_semantic_aliases(source_text)))
    best, best_score = None, 0
    for ep in extra_props:
        ep_tok = set(_tokenize_words(_apply_semantic_aliases(ep)))
        score = len(text_tok & ep_tok)
        if score > best_score:
            best, best_score = ep, score
    if best_score > 0: return best
    if fallback_field:
        cand = f"{fallback_field}_Extra"
        for ep in extra_props:
            if _normalize_token(ep) == _normalize_token(cand):
                return ep
    return None

def _generic_extra_retarget_meta(meta: dict, source_text: str) -> dict:
    """
    If a variable currently points to the main array but semantically looks like a child extra,
    push it into the Extra child of that same parent item (generic and works for Menstrual & XRays).
    If the schema clearly says the field is a main property, keep it in the parent item.
    An optional min_instance hint is observed ONLY when we are retargeting (not when the child leaf is explicit).
    """
    g = _find_generic_extra_for(meta)
    if not g: return meta
    ap = list(meta.get("array_path") or [])
    if ap != [g["main_array"]]: return meta
    sf = meta.get("schema_field")

    # keep bona fide parent fields in the parent item
    if sf in g["main_props"]:
        return meta

    # if we are retargeting, honour a "min instance for extra" hint (if present)
    idxs = list(meta.get("indices") or [])
    parent_idx = int(idxs[0]) if idxs else 1
    min_inst = g.get("min_instance_for_extra")
    if isinstance(min_inst, int) and parent_idx < min_inst:
        # Below the suggested threshold: keep it parent to avoid discarding early-instance data
        return meta

    # Try mapping to a concrete child field
    child_field = _choose_extra_field_from_text(g["extra_props"], source_text, sf)
    if not child_field:
        return meta

    child_idx = _secondary_index_from_text(source_text)
    return {
        **meta,
        "array_path": [g["main_array"], g["extra_array"]],
        "array_name": g["extra_array"],
        "schema_field": child_field,
        "indices": [parent_idx, child_idx],
        "entry_num": child_idx,
    }



def _materialize_registry_accepting_all_keys(varflag_map: Dict[str, str],
                                             token_index: Dict[str, dict],
                                             schemas_by_slug: Dict[str, dict]) -> Dict[str, dict]:
    """
    Ensure the dynamic registry is built and available.

    This helper is called lazily by `rename_variable` so that notebooks
    do not have to explicitly initialise the registry in trivial use cases.
    """

    registry: Dict[str, dict] = {}
    asd_orders = _compute_asd_ageband_orders(varflag_map, token_index, schemas_by_slug)
    orders_pa = _compute_pa_ageband_orders(varflag_map, token_index, schemas_by_slug)
    asd_orders = {**(asd_orders or {}), **(orders_pa or {})}

    for raw_var, desc in (varflag_map or {}).items():
        base, v_nums = _split_descriptor(desc)
        leaf = _token_match(base, token_index)
        if not leaf: continue

        array_path = list(leaf["array_path"])
        indices, label = _compose_indices(
            v_nums, leaf, desc, schemas_by_slug,
            asd_orders=asd_orders, base_token=desc
        )
        meta = {
            "section": leaf["section"],
            "array_name": leaf["array_name"],
            "entry_num": indices[-1] if indices else None,
            "schema_field": leaf["schema_field"],
            "array_path": array_path,
            "indices": indices,
        }
        if label is not None:
            meta["index_label"] = label

        # Section-specific retarget
        meta = _xray_retarget_meta(meta, f"{desc} {raw_var}")
        meta = _generic_extra_retarget_meta(meta, f"{desc} {raw_var}")


        if meta.get("section") in ("physical_dev", "physicaldevelopment"):
            if (meta.get("array_path") or [None])[0] == PD_ARRAY:
                meta["num_field"] = PD_NUM_FIELD
            elif (meta.get("array_path") or [None])[0] == HD_ARRAY:
                meta["num_field"] = HD_NUM_FIELD
            elif (meta.get("array_path") or [None])[0] == BRA_ARRAY:
                meta["num_field"] = BRA_NUM_FIELD

        if meta.get("section") in ("physical_dev", "physicaldevelopment") and meta.get("num_field"):
            _pd_dbg("REG.materialize PD meta",
                    {"raw": raw_var, "desc": desc, "array": meta.get("array_name"),
                     "indices": meta.get("indices"), "index_label": meta.get("index_label"),
                     "num_field": meta.get("num_field"), "schema_field": meta.get("schema_field")})

        # XRays → ChestXrayEvents uses R0_ChestXray_Num
        if meta.get("section") in ("xrays", "xray"):
            if (meta.get("array_path") or [None])[0] == CX_ARRAY:
                meta["num_field"] = CX_NUM_FIELD

        if meta.get("section") in ("other_lifestyle_factors"):
            if (meta.get("array_path") or [None])[0] == NW_ARRAY:
                meta["num_field"] = NW_NUM_FIELD

        # raw key
        registry[str(raw_var).strip()] = meta
        # VarDesc key
        registry[str(desc).strip()] = meta

        if meta["indices"] and meta["array_path"]:
            registry[f"{meta['schema_field']}{meta['indices'][-1]}"] = meta
        # bare schema leaf (R0_<Leaf>)
        bare = {**meta, "entry_num": 1, "indices": [1] * len(meta.get("array_path") or [])}
        if label is not None:
            bare["index_label"] = label
        registry[leaf['schema_field']] = bare

    return registry

def _resolve_on_the_fly(key: str, token_index: Dict[str, dict], schemas_by_slug: Dict[str, dict]) -> Optional[dict]:
    base_token, nums = _split_descriptor(key)
    if not base_token:
        return None
    leaf = _token_match(base_token, token_index)
    if not leaf:
        return None
    array_path = list(leaf["array_path"])
    indices, label = _compose_indices(nums, leaf, key, schemas_by_slug, asd_orders=None, base_token=base_token)
    meta = {
        "section": leaf["section"],
        "array_name": leaf["array_name"],
        "entry_num": indices[-1] if indices else None,
        "schema_field": leaf["schema_field"],
        "array_path": array_path,
        "indices": indices,
    }

    if label is not None:
        meta["index_label"] = label
    # Section-specific retarget
    meta = _xray_retarget_meta(meta, key)
    meta = _generic_extra_retarget_meta(meta, key)

    if meta.get("section") in ("physical_dev", "physicaldevelopment") and meta.get("num_field"):
        _pd_dbg("RESOLVE.onthefly PD meta",
                {"key": key, "array": meta.get("array_name"),
                 "indices": meta.get("indices"), "index_label": meta.get("index_label"),
                 "num_field": meta.get("num_field"), "schema_field": meta.get("schema_field")})
                 
    if meta.get("section") in ("xrays", "xray"):
        if (meta.get("array_path") or [None])[0] == CX_ARRAY:
            meta["num_field"] = CX_NUM_FIELD

    return meta

