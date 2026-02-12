"""
Core utilities for questionnaire ETL.

This module is the main "glue" of the pipeline. It handles:
- Loading configuration and paths from a JSON config file.
- Pulling raw questionnaire responses from SQL and pivoting them to
  a StudyID x VariableName wide format.
- Loading JSON Schemas (including resolving $ref references).
- Cleaning and transforming raw values using schema constraints.
- Assembling flat or nested records ready for restructuring.
- Validating final JSON documents against their schemas.
- Saving output JSON and change-tracking artefacts to disk.

All section-specific ETL notebooks (`*_Load.ipynb`) use functions in
this module to perform the heavy lifting.
"""

import os
import sys
import json
import re
import pandas as pd
from collections import defaultdict
from jsonschema.validators import validator_for
from jsonschema import Draft202012Validator, validate, FormatChecker, RefResolver
from concurrent.futures import ThreadPoolExecutor
import time

sys.path.append(os.path.abspath("N:\\CancerEpidem\\BrBreakthrough\\DeliveryProcess\\Schema_and_Derivation_utils\\Questionnaire\\R0\\scripts"))
import cleaning_utils as cr
from nested_utils import rename_variable
from pseudo_anon_utils import load_sid_codes, process_dates, pseudo_anonymize_studyid
from cleaning_utils import rules, convert_to_date
from nested_utils import rename_variable

sys.path.append(os.path.abspath("N:\\CancerEpidem\\BrBreakthrough\\DeliveryProcess\\Schema_and_Derivation_utils"))
from utilities import connect_DB, createLogger, read_data
from config import r0_json_path, r0_json_path_pii, ct_path 

def get_config():
    """
    Load ETL configuration from a JSON file.

    The config typically contains:
    - database connection parameters
    - base paths for schemas, outputs, and validation artefacts
    - any section-specific settings required by the notebooks.
    """
    return {
        'Delivery_log_path': 'N:\CancerEpidem\BrBreakthrough\DeliveryProcess\Logs',
        'test_server': 'DoverVTest',
        'live_server': 'DoverV',
        'r0_json_path': 'N:\\CancerEpidem\\BrBreakthrough\\DeliveryProcess\\Schema_and_Derivation_utils\\Questionnaire\\R0\\json_schemas',
        'out_json_path': 'N:\\CancerEpidem\\BrBreakthrough\\DeliveryProcess\\Data_Output_Testing'
    }

def load_and_pivot_data(question_range, logger):
    """
    Load raw questionnaire responses for a given question range and pivot wide.

    This function:
    - Builds a SQL query that pulls the appropriate combination of
      GeneralResponses and any nested tables (Pregnancies, Relatives, etc.)
      based on the supplied `question_range`.
    - Executes the query against QuestTransformed.
    - Joins in question metadata and VarFlagging PII flags.
    - Pivots the long format (StudyID, VariableName, ResponseText)
      into a wide DataFrame indexed by StudyID.

    Parameters
    ----------
    question_range : str
        SQL fragment for the QuestionID filter (e.g. 'BETWEEN 2150 AND 2260').
    logger :
        Logger instance for progress / debug messages.

    Returns
    -------
    (pd.DataFrame, pd.DataFrame, pd.DataFrame)
        - Wide pivoted DataFrame of responses.
        - Questions metadata.
        - VarFlagging (dfPII) metadata.
    """
    config = get_config()
    dm_conn = connect_DB('QuestTransformed', config['live_server'], logger)
    
    if question_range == 'BETWEEN 550 AND 739':
        base_query = '''
            SELECT [StudyID], [VariableName], [ResponseText]
            FROM [QuestTransformed].[dbo].[GeneralResponses] 
            WHERE RoundID = 1 AND QuestionID {0}
            UNION ALL
            SELECT [StudyID], [VariableName], [ResponseText]
            FROM [QuestTransformed].[dbo].[NestedPregnancies] 
            WHERE RoundID = 1 AND QuestionID {0}
        '''
    elif question_range == 'BETWEEN 400 AND 544':
        base_query = '''
            SELECT [StudyID], [VariableName], [ResponseText]
            FROM [QuestTransformed].[dbo].[GeneralResponses] 
            WHERE RoundID = 1 AND QuestionID {0}
            UNION ALL
            SELECT [StudyID], [VariableName], [ResponseText]
            FROM [QuestTransformed].[dbo].[NestedOvaryOperations] 
            WHERE RoundID = 1 AND QuestionID {0}
        '''
    elif question_range == 'BETWEEN 801 AND 1015':
        base_query = '''
            SELECT [StudyID], [VariableName], [ResponseText]
            FROM [QuestTransformed].[dbo].[GeneralResponses] 
            WHERE RoundID = 1 AND QuestionID {0}
            UNION ALL
            SELECT [StudyID], [VariableName], [ResponseText]
            FROM [QuestTransformed].[dbo].[NestedHormoneDrugs] 
            WHERE RoundID = 1 AND QuestionID {0}
        '''
    elif question_range == 'BETWEEN 1111 AND 1185 OR QuestionID BETWEEN 1366 AND 1370':
        base_query = '''
            SELECT [StudyID], [VariableName], [ResponseText]
            FROM [QuestTransformed].[dbo].[GeneralResponses] 
            WHERE RoundID = 1 AND QuestionID {0}
            UNION ALL
            SELECT [StudyID], [VariableName], [ResponseText]
            FROM [QuestTransformed].[dbo].[NestedBreastDisease] 
            WHERE RoundID = 1 AND QuestionID {0}
        '''
    elif question_range == 'BETWEEN 1186 AND 1250':
        base_query = '''
            SELECT [StudyID], [VariableName], [ResponseText]
            FROM [QuestTransformed].[dbo].[GeneralResponses] 
            WHERE RoundID = 1 AND QuestionID {0}
            UNION ALL
            SELECT [StudyID], [VariableName], [ResponseText]
            FROM [QuestTransformed].[dbo].[NestedCancers] 
            WHERE RoundID = 1 AND QuestionID {0}
        '''
    elif question_range == 'BETWEEN 2050 AND 2070':
        base_query = '''
            SELECT [StudyID], [VariableName], [ResponseText]
            FROM [QuestTransformed].[dbo].[GeneralResponses] 
            WHERE RoundID = 1 AND QuestionID {0}
            UNION ALL
            SELECT [StudyID], [VariableName], [ResponseText]
            FROM [QuestTransformed].[dbo].[NestedJobs] 
            WHERE RoundID = 1 AND QuestionID {0}
        '''
    elif question_range == 'BETWEEN 2500 AND 2734':
        base_query = '''
            SELECT [StudyID], [VariableName], [ResponseText]
            FROM [QuestTransformed].[dbo].[GeneralResponses] 
            WHERE RoundID = 1 AND QuestionID {0}
            UNION ALL
            SELECT [StudyID], [VariableName], [ResponseText]
            FROM [QuestTransformed].[dbo].[NestedRelatives] 
            WHERE RoundID = 1 AND QuestionID {0}
            UNION ALL
            SELECT [StudyID], [VariableName], [ResponseText]
            FROM [QuestTransformed].[dbo].[NestedRelativesCancers] 
            WHERE RoundID = 1 AND QuestionID {0}
        '''

    elif question_range == 'BETWEEN 1400 AND 1416':
        base_query = '''
            SELECT [StudyID], [VariableName], [ResponseText]
            FROM [QuestTransformed].[dbo].[GeneralResponses] 
            WHERE RoundID = 1 AND QuestionID {0}
            UNION ALL
            SELECT [StudyID], [VariableName], [ResponseText]
            FROM [QuestTransformed].[dbo].[NestedCancers] 
            WHERE RoundID = 1 AND QuestionID {0}
        '''

    elif question_range == 'BETWEEN 1600 AND 1742':
        base_query = '''
            SELECT [StudyID], [VariableName], [ResponseText]
            FROM [QuestTransformed].[dbo].[GeneralResponses] 
            WHERE RoundID = 1 AND QuestionID {0}
            UNION ALL
            SELECT [StudyID], [VariableName], [ResponseText]
            FROM [QuestTransformed].[dbo].[NestedDrugs] 
            WHERE RoundID = 1 AND QuestionID {0}
        '''

    else:
        base_query = '''
            SELECT [StudyID], [VariableName], [ResponseText]
            FROM [QuestTransformed].[dbo].[GeneralResponses] 
            WHERE RoundID = 1 AND QuestionID {}
        '''
    
    queries = {
        'main': base_query.format(question_range),
        'questions': f'SELECT * FROM [QuestTransformed].[dbo].[Questions] WHERE RoundID = 1 AND QuestionID {question_range}',
        'pii': f'SELECT * FROM [QuestTransformed].[dbo].[VariableFlagging] WHERE QuestionID {question_range}'
    }
    
    df = read_data(queries['main'], dm_conn, logger)
    dfQuest = read_data(queries['questions'], dm_conn, logger)
    dfPII = read_data(queries['pii'], dm_conn, logger)
    
    merged = pd.merge(df, dfQuest[['VariableName', 'Section', 'QuestionTypeID']], on='VariableName', how='left')
    pivoted = pd.pivot(merged, index='StudyID', columns='VariableName', values='ResponseText').fillna('')
    return pivoted, dfPII

def load_schema(schema_path, schema_name):
    """
    Load a JSON Schema file and resolve any $ref references.

    Parameters
    ----------
    schema_path : str
        Path to the root schema JSON file.
    schema_name : str, optional
        Optional name for logging or internal reference.

    Returns
    -------
    dict
        Fully-resolved JSON Schema ready for validation and traversal.
    """
    path = os.path.join(schema_path, f'{schema_name}.json')
    with open(path, 'r') as f:
        schema = json.load(f)
    
    # Only resolve if $ref exists
    if any("$ref" in item for item in json.dumps(schema)):
        return resolve_references(schema, schema_path)
    return schema

def resolve_references(schema, base_path):
    """
    Recursively resolve JSON Schema $ref references.

    This uses jsonschema's RefResolver to replace $ref nodes with their
    target definitions so downstream code can traverse the schema without
    having to handle references manually.
    """
    resolver = RefResolver(
        base_uri=f"file://{base_path}/",
        referrer=schema,
        cache_remote=True
    )
    
    def _resolve(node):
        if isinstance(node, dict) and "$ref" in node:
            path = node["$ref"]
            with resolver.resolving(path) as resolved:
                return _resolve(resolved)
        elif isinstance(node, dict):
            return {k: _resolve(v) for k, v in node.items()}
        elif isinstance(node, list):
            return [_resolve(item) for item in node]
        return node
    
    return _resolve(schema)

def clean_flat_value(value, var_name, field_name, constraints, expected_type):
    """
    Clean a simple (non-nested) value according to schema constraints.

    This applies:
    - Any hard-coded recodes from `newValMap` (via `get_newvalmap_value`).
    - Generic cleaning rules from `cleaning_utils.rules` for numeric types.
    - Basic null / missing handling.

    Returns a value suitable for direct inclusion in the JSON payload.
    """
    # Apply newValMap conversions
    mapped_value = get_newvalmap_value(var_name, value, field_name)
    if mapped_value is not None:
        value = mapped_value

    # Handle empty values
    if value is None or (isinstance(value, str) and value.strip() in ('', 'null')):
        return None

    # Apply type-specific cleaning
    min_val = constraints.get('min')
    max_val = constraints.get('max')
    cleaned_value = cr.rules(value, expected_type, min_val, max_val)
    
    # Handle enum constraints
    enum_vals = constraints.get('enum')
    if enum_vals is not None and cleaned_value is not None:
        # Convert to same type for comparison
        try:
            if expected_type == "integer":
                cleaned_value = int(cleaned_value)
        except (ValueError, TypeError):
            pass
            
        if cleaned_value not in enum_vals:
            return None  # Invalid value per enum constraint

    return cleaned_value

def convert_nested_value(value, var_name, field_name, var_type_map, constraint_map, newValMap):
    """
    Convert a value that will ultimately sit inside a nested structure.

    This mirrors `clean_flat_value` but is used when the same raw variable
    may appear multiple times inside arrays (e.g. pregnancy episodes).
    """

    # newValMap lookup
    mapped_value = get_newvalmap_value(var_name, value, field_name)
    if mapped_value is not None:
        return mapped_value
    
    # Get expected type
    expected_type = 'string'  # Default
    if field_name and field_name in var_type_map:
        expected_type = var_type_map[field_name]
    elif var_name in var_type_map:
        expected_type = var_type_map[var_name]
    
    # Get min/max constraints
    min_val, max_val = None, None
    if field_name and field_name in constraint_map:
        min_val = constraint_map[field_name]["min"]
        max_val = constraint_map[field_name]["max"]
    elif var_name in constraint_map:
        min_val = constraint_map[var_name]["min"]
        max_val = constraint_map[var_name]["max"]
    
    # Use CleaningRules
    cleaned_value = rules(value, expected_type, min_val, max_val)

    # Get enum if available
    enum_vals = None
    if field_name and field_name in constraint_map:
        enum_vals = constraint_map[field_name].get('enum')

    # Enforce enum: if cleaned_value not allowed, null it
    if enum_vals is not None and cleaned_value is not None:
        try:
            # coerce for integer enums
            if var_type_map.get(field_name) == "integer":
                cleaned = int(cleaned_value)
            else:
                cleaned = cleaned_value
        except (ValueError, TypeError):
            cleaned = cleaned_value

        if cleaned not in enum_vals:
            return None

    # Final guardrails: coerce to the expected numeric type if still stringy
    if expected_type == 'integer':
        if isinstance(cleaned_value, str):
            s = cleaned_value.strip()
            # allow "123" or "123.0" etc. (but not real decimals)
            try:
                f = float(s)
                if f.is_integer():
                    return int(f)
            except Exception:
                pass
    elif expected_type == 'number':
        if isinstance(cleaned_value, str):
            try:
                return float(cleaned_value.strip())
            except Exception:
                pass

    return cleaned_value


def process_nested_data(raw_data, variable_mapping, var_type_map, constraint_map, newValMap, keep_raw_keys=True):
    """
    Process section data that contains nested arrays.

    This function:
    - Pre-resolves the mapping from raw variable names to schema fields
      and array locations using `nested_utils.rename_variable`.
    - Cleans values using `convert_nested_value`.
    - Builds intermediate records that still carry enough information
      to be turned into fully nested JSON by `restructure_utils`.

    It also produces change-tracking information for later QC.
    """

    processed_data = []
    change_tracking = {}

    # Collect the unique raw variable names
    unique_raw_vars = set()
    for _pid, responses in raw_data.items():
        unique_raw_vars.update(responses.keys())

    # Resolve unique raw vars ONCE
    # Store meta: { raw -> (schema_field, array_path_len) }
    resolved_info = {}
    for var_name in unique_raw_vars:
        schema_field = None
        array_path_len = 0

        # prefer explicit mapping if provided
        mapped = variable_mapping.get(var_name) if isinstance(variable_mapping, dict) else None
        if isinstance(mapped, dict):
            schema_field = mapped.get("schema_field")
            ap = mapped.get("array_path") or []
            array_path_len = len(ap)
        else:
            try:
                meta = rename_variable(var_name)
            except Exception:
                meta = None
            if isinstance(meta, dict):
                schema_field = meta.get("schema_field")
                if meta.get("array_path"):
                    array_path_len = len(meta["array_path"])
                elif meta.get("array_name") is not None:
                    array_path_len = 1

        resolved_info[var_name] = (schema_field, array_path_len)

    # Process each participant without additional resolver calls
    for participant_id, responses in raw_data.items():
        participant_data = {"StudyID": participant_id}

        for var_name, raw_value in responses.items():
            schema_field, array_path_len = resolved_info.get(var_name, (None, 0))

            # Clean/convert using existing conversion (uses field_name for types/ranges)
            cleaned_value = convert_nested_value(
                raw_value,
                var_name,
                schema_field,
                var_type_map,
                constraint_map,
                newValMap
            )

            # Change tracking
            if str(raw_value) != str(cleaned_value):
                ct_var = change_tracking.setdefault(var_name, {})
                entry = ct_var.setdefault(raw_value, {"new_value": cleaned_value, "count": 0})
                entry["count"] += 1

            # Keep raw key for downstream array placement
            if keep_raw_keys:
                participant_data[var_name] = cleaned_value

            # If this maps to a top-level (non-array) schema leaf, write to final key now
            if schema_field and array_path_len == 0:
                # (top-level scalar; promote to final R0_* name)
                participant_data[schema_field] = cleaned_value

            # For array-mapped fields, do NOT promote here; restructure will place them into the correct array/indices using the raw key mapping.

        processed_data.append(participant_data)

    print(f"[process_nested_data] Processed {len(processed_data)} participants; "
          f"{len(resolved_info)} unique variables resolved once.")
    return processed_data, change_tracking

def extract_schema_constraints(schema):
    """
    Extract min/max/enum constraints and type information from a schema.

    Returns a pair of dicts:
    - constraint_map: {field_name: {"minimum": ..., "maximum": ..., "enum": ...}}
    - var_type_map: {field_name: json_type}
    """
    constraint_map = {}
    var_type_map = {}

    def record_field(field, cfg):
        # ---- type (strip 'null') ----
        ftype = cfg.get('type', 'string')
        if isinstance(ftype, list):
            ftype = [t for t in ftype if t != 'null']
            ftype = ftype[0] if ftype else 'string'
        var_type_map[field] = ftype

        # ---- constraints ----
        min_val = cfg.get('minimum')
        max_val = cfg.get('maximum')
        enum_vals = cfg.get('enum')

        # also consider anyOf/oneOf branches
        for key in ('anyOf', 'oneOf'):
            if key in cfg:
                for sub in cfg[key]:
                    if min_val is None and 'minimum' in sub:
                        min_val = sub['minimum']
                    if max_val is None and 'maximum' in sub:
                        max_val = sub['maximum']
                    if enum_vals is None and 'enum' in sub:
                        enum_vals = sub['enum']

        constraint_map[field] = {'min': min_val, 'max': max_val, 'enum': enum_vals}

    def walk(node):
        if not isinstance(node, dict):
            return

        # If this node itself looks like a leaf field (has a concrete type), don't
        # know its field-name here; the field-name is provided by the parent props loop,
        # so only 'record_field' from within the props iteration below.

        # Walk object properties
        props = node.get('properties')
        if isinstance(props, dict):
            for key, cfg in props.items():
                # record THIS field (leaf or container) to capture type/constraints of leaves
                if isinstance(cfg, dict) and 'type' in cfg:
                    record_field(key, cfg)
                # then recurse into children (objects/arrays)
                walk(cfg)

        # Walk array items
        items = node.get('items')
        if isinstance(items, dict):
            walk(items)

    walk(schema)
    return constraint_map, var_type_map

def clean_value(value, var_name, field_name, constraints, expected_type, newValMap=None):
    """
    High-level cleaner used by nested processing.

    This wraps `clean_flat_value`/`convert_nested_value`, looking up
    the expected type and constraints for `field_name` from the maps
    produced by `extract_schema_constraints`.
    """
    # Handle empty values
    if value is None:
        return None
    if isinstance(value, str) and value.strip() in ['', 'null']:
        return None
    
    # Pre-map values using newValMap if available
    mapped_value = value
    if newValMap:
        if field_name in newValMap and value in newValMap[field_name]:
            mapped_value = newValMap[field_name][value]
        elif var_name in newValMap and value in newValMap[var_name]:
            mapped_value = newValMap[var_name][value]
    
    # Get constraints
    min_val = constraints.get('min')
    max_val = constraints.get('max')
    enum_vals = constraints.get('enum')
    
    # Apply type-specific cleaning to the MAPPED VALUE
    cleaned_value = rules(mapped_value, expected_type, min_val, max_val)
    
    # Only enforce enum constraints if they exist
    if enum_vals is not None and cleaned_value is not None:
        try:
            if expected_type == "integer":
                cleaned_value = int(cleaned_value)
        except (ValueError, TypeError):
            pass
            
        if cleaned_value not in enum_vals:
            return None
    
    return cleaned_value

# Value mapping
def get_newvalmap_value(var_name, value, field_name):
    """
    Look up a recoded value from the global `newValMap`.

    Either the raw variable name or the schema field name can be used
    as the key. If no mapping exists for the value, the original is returned.
    """
    # Check field-specific mappings
    if field_name in cr.newValMap and value in cr.newValMap[field_name]:
        return cr.newValMap[field_name][value]
    
    # Check variable-specific mappings
    if var_name in cr.newValMap and value in cr.newValMap[var_name]:
        return cr.newValMap[var_name][value]
    
    return None

def validate_data(data, schema, schema_path=None):
    """
    Validate a list of JSON-like records against a JSON Schema.

    Uses jsonschema's `validator_for` to build an appropriate validator,
    then iterates over each record and collects validation errors.

    Parameters
    ----------
    data : list[dict]
        Records to validate.
    schema : dict
        JSON Schema.
    schema_path : str, optional
        Path used only for clearer error messages.
    logger :
        Optional logger for progress.
    max_errors : int
        Maximum number of errors to collect before stopping.

    Returns
    -------
    list[jsonschema.ValidationError]
        List of validation errors (possibly truncated).
    """
    start_time = time.time()
    
    # Create resolver
    resolver = None
    if schema_path:
        # Handle Windows paths by converting to absolute and URI format
        base_path = os.path.dirname(os.path.abspath(schema_path))
        base_uri = f"file:///{base_path}/".replace("\\", "/")
        resolver = RefResolver(base_uri, schema, cache_remote=True)
    
    # Compile validator
    ValidatorClass = validator_for(schema)
    validator = ValidatorClass(
        schema, 
        resolver=resolver,
        format_checker=FormatChecker()
    )
    
    # Handle array-based schemas
    if isinstance(data, list):
        total_items = len(data)
        print(f"Validating {total_items:,} items...")
        
        error_count = 0
        last_log_time = start_time
        last_log_count = 0
        
        print("0%", end="", flush=True)
        
        for i, item in enumerate(data):
            errors = list(validator.iter_errors(item))
            if errors:
                error_count += 1
                if error_count <= 5:
                    error_fields = set()
                    for error in errors:
                        # Extract field path from validation error
                        path = list(error.absolute_path)
                        if path:
                            error_fields.add('.'.join(str(p) for p in path))
                    
                    fields_str = ', '.join(error_fields) or 'unknown field'
                    print(f"\nError at index {i} (fields: {fields_str}): {errors[0].message}")
            
            # Progress update
            current_time = time.time()
            if (current_time - last_log_time > 5) or (i/total_items - last_log_count > 0.1):
                percent = (i / total_items) * 100
                print(f"\r{percent:.1f}%", end="", flush=True)
                last_log_time = current_time
                last_log_count = i/total_items
        
        print(f"\r100% - Validation completed in {time.time() - start_time:.2f} seconds")
        if error_count == 0:
            print("✓ All items are valid")
        else:
            print(f"✗ Validation failed with {error_count} errors")
            if error_count > 5:
                print(f"(First 5 errors shown, {error_count-5} additional errors not displayed)")
        return
    
    # Fallback to standard validation for non-array data
    try:
        print("Validating single object...")
        validator.validate(data)
        print('✓ JSON data is valid.')
    except Exception as e:
        print('✗ Validation Error:', e)

# Save processed data
def save_output(data, output_name, logger, stage=None):
    config = get_config()
    base_out = config['out_json_path']
    if stage:
        out_dir = os.path.join(base_out, stage)
    else:
        out_dir = base_out

    os.makedirs(out_dir, exist_ok=True)
    output_path = os.path.join(out_dir, f'{output_name}.json')
    
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=4)
    
    logger.info(f"Saved output to {output_path}")
    print(f"Output saved: {output_path}")

def save_change_tracking(change_tracking, section_name, logger):
    os.makedirs(ct_path, exist_ok=True)
    file_path = os.path.join(ct_path, f"{section_name}_ChangeTracking.json")
    with open(file_path, "w") as f:
        json.dump(change_tracking, f, indent=2)
    logger.info(f"Saved change tracking to {file_path}")
    print(f"Change tracking saved: {file_path}")

# --- NEW helper: collect PII schema-field keys from dfPII ---
def _collect_pii_schema_fields(dfPII, schema):
    """
    Build a set of schema leaf keys that must be removed from data (e.g., 'R0_XrayHospital_Extra').
    Uses rename_variable to translate raw VariableName (e.g., 'xradhosp4', 'Q9_4_1_3') into schema keys.
    Falls back to mapping via top-level schema 'name' if rename_variable can't resolve.
    """
    pii_mask = dfPII[dfPII['PII'] == 1]
    pii_vars = set(pii_mask['VariableName'])

    # Detect if schema is array or object and take the right properties block
    is_array_schema = "items" in schema and "properties" in schema["items"]
    properties = schema["items"]["properties"] if is_array_schema else schema.get("properties", {})

    # Build a quick lookup from 'name' -> json key (top-level only)
    name_to_key_map = {}
    for key, props in properties.items():
        if isinstance(props, dict) and "name" in props:
            name_to_key_map[props["name"]] = key

    pii_schema_fields = set()
    for var in pii_vars:
        meta = rename_variable(var)
        if meta and meta.get('schema_field'):
            pii_schema_fields.add(meta['schema_field'])
        elif var in name_to_key_map:
            pii_schema_fields.add(name_to_key_map[var])
        else:
            # As a fallback, assume the VariableName is already a schema key
            pii_schema_fields.add(var)
            print(f"Warning: No schema mapping found for PII variable {var}")

    # Expand families so 'R0_XrayHospital' also removes 'R0_XrayHospital_Extra' (and vice-versa)
    expanded = set(pii_schema_fields)
    for key in list(pii_schema_fields):
        if key.endswith("_Extra"):
            base = key[:-6]
            expanded.add(base)
        else:
            expanded.add(f"{key}_Extra")
    return expanded, pii_vars


# Recursive in-place remover
def _remove_pii_inplace(node, pii_keys):
    """
    Recursively delete any dict keys that are in pii_keys, at any depth.
    Traverses dicts and lists in-place.
    """
    if isinstance(node, dict):
        for k in list(node.keys()):
            if k in pii_keys:
                node.pop(k, None)
            else:
                _remove_pii_inplace(node[k], pii_keys)
    elif isinstance(node, list):
        for item in node:
            _remove_pii_inplace(item, pii_keys)


# Recursive PII masking that reaches nested arrays/objects
def mask_pii(data, dfPII, schema):
    """
    Remove PII fields from the entire data structure (top-level and nested).
    Returns (data, pii_vars).
    """
    pii_schema_fields, pii_vars = _collect_pii_schema_fields(dfPII, schema)

    # Scrub every record deeply
    for record in data:
        _remove_pii_inplace(record, pii_schema_fields)

    print("PII masking completed (deep).")
    return data, pii_vars

    
def apply_pseudo_anonymization(data, server, logger, schema=None, date_dict=None):
    """
    Apply pseudo-anonymization to processed data (list[dict]).
    - Keeps 'data' as nested dict/list
    - Passes required schema + dateDict
    """
    # Load SID codes
    sid_df = load_sid_codes(server, logger)

    # Ensure we have schema + dateDict
    if schema is None:
        # Load section schemas as pipeline currently does
        # e.g., schema = load_schema(config['r0_json_path'], 'BreastCancer') for the BC section
        pass
    if date_dict is None:
        from pseudo_anon_utils import dateDict as DATECFG
        date_dict = DATECFG

    # Operate on list[dict], not a DataFrame
    processed_with_dates = process_dates(data, sid_df, schema, logger, date_dict)

    # Finally pseudo-anonymize StudyIDs
    return pseudo_anonymize_studyid(processed_with_dates, sid_df)


# Bridge VariableFlagging (SQL) → nested_utils resolver
def init_varresolver_from_dfPII(dfPII, schema, section_name):
    """
    Wire nested_utils to the VariableFlagging rows already loaded from SQL (dfPII)
    so rename_variable(var_name) can route raw names into arrays.

    section_name: 'PhysicalDevelopment' | 'Pregnancies' | 'XRays' | 'BreastCancer' | 'MenstrualMenopause'
    """
    import nested_utils as nv

    # Build raw -> VariableDesc dict from SQL table
    if 'VariableName' not in dfPII.columns or 'VariableDesc' not in dfPII.columns:
        raise ValueError("dfPII must include columns: VariableName, VariableDesc")
    varflag = (
        dfPII[['VariableName', 'VariableDesc']]
        .dropna(subset=['VariableName', 'VariableDesc'])
        .astype(str)
        .set_index('VariableName')['VariableDesc']
        .to_dict()
    )

    # Map section to slug used by nested_utils
    slug_map = {
        "GeneralInformation": "general_information",
        "PhysicalDevelopment": "physical_dev",
        "Pregnancies": "pregnancies",
        "XRays": "xrays",
        "BreastCancer": "breast_cancer",
        "BreastDisease": "breast_disease",
        "MenstrualMenopause": "menstrual_menopause",
        "Jobs": "jobs",
        "ContraceptiveHRT": "contraceptive_hrt",
        "PhysicalActivity": "physical_activity",
        "CancerRelatives": "cancer_relatives",
        "MH_Illnesses": "mh_illnesses",
        "BirthDetails": "birth_details",
        "Mammograms": "mammograms",
        "MH_CancersBenignTumors": "cancers_benign_tumors",
        "MH_DrugsSupplements": "drugs_supplements",
        "OtherBreastSurgery": "other_breast_surgery",
        "OtherLifestyleFactors": "other_lifestyle_factors"
    }
    slug = slug_map.get(section_name, section_name.lower())

    # Initialize resolver with just this section's schema
    nv.init_dynamic_registry(
        varflag=varflag,
        schemas_by_slug={slug: schema}
    )