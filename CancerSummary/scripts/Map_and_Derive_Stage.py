# -*- coding: utf-8 -*-
"""
Created on Thu Sep 11 14:50:18 2025

@author: shegde
purpose: Set of functions used to map source variables to Registry standard and derive stage variable
"""

import os
os.chdir('N:\CancerEpidem\BrBreakthrough\SHegde\Schema_and_Derivation_utils\CancerSummary\scripts')
import sys
sys.path.append(os.path.abspath('N:\CancerEpidem\BrBreakthrough\SHegde\Schema_and_Derivation_utils'))

import pandas as pd


def build_enum_mapping(source_enum, target_enum, special_rules=None):
    """
    Build mapping dictionary from source enum values to target enum values.

    Args:
        source_enum (list): List of source enum values
        target_enum (list): List of target enum values
        special_rules (dict): Optional dictionary of special mappings

    Returns:
        dict: Mapping from source to target values
    """
    mapping = {}
    for val in source_enum:
        if val is None and special_rules and None in special_rules:
            mapping[val] = special_rules[None]
        elif val in target_enum:
            mapping[val] = val
        elif special_rules and val in special_rules:
            mapping[val] = special_rules[val]
        else:
            mapping[val] = None  # fallback if no match
    return mapping


def map_variable(df, source_col, source_schema, target_schema, target_col=None, special_rules=None):
    """
    Map a variable's values from source schema enums to target schema enums.

    Args:
        df (pd.DataFrame): Input dataframe
        source_col (str): Column in dataframe to map
        source_schema (dict): Source schema (must include 'enum')
        target_schema (dict): Target schema (must include 'enum')
        target_col (str): Name for mapped column (default: source_col + '_TARGET')
        special_rules (dict): Optional dictionary for manual enum mappings

    Returns:
        pd.DataFrame: Dataframe with new mapped column
    """
    if target_col is None:
        target_col = f"{source_col}_TARGET"

    mapping = build_enum_mapping(
        source_schema["enum"],
        target_schema["enum"],
        special_rules=special_rules
    )

    df[target_col] = df[source_col].map(mapping)
    
    changed = df[source_col] != df[target_col]
    change_count = changed.sum()
    
    df[source_col] = df[target_col]
    
    return df, mapping, int(change_count), target_col


def harmonize_source(df, source_schema, target_schema, variables, logger, special_rules=None):
    """
    Harmonize multiple variables from a single source schema to a target schema.

    Args:
        df (pd.DataFrame): Source dataframe
        source_schema (dict): Source schema {var: {enum: [...]}}
        target_schema (dict): Target schema {var: {enum: [...]}}
        variables (dict): {source_var: target_var}
        special_rules (dict): {source_var: {source_val: target_val}}
        logger (object): Logging object for log statement

    Returns:
        pd.DataFrame: Harmonized dataframe
        dict: {source_var: {"target_var": str, "mapping": dict, "changed_rows": int}}
    """
    mappings_used = {}

    for src_var, tgt_var in variables.items():
        
        if src_var not in source_schema["properties"] or tgt_var not in target_schema["properties"]:
            logger.warning("Skipping " + str(src_var) + ":" + str(tgt_var) + " (not in schema)")
            continue

        rules = None
        if special_rules and src_var in special_rules:
            rules = special_rules[src_var]

        df, mapping, changed_count, mapped_col = map_variable(
            df,
            source_col = src_var,
            target_col = tgt_var,
            source_schema = source_schema["properties"][src_var],
            target_schema = target_schema["properties"][tgt_var],
            special_rules = rules
        )

        mappings_used[src_var] = {
            "target_var": tgt_var,
            "mapped_col": mapped_col,
            "mapping": mapping,
            "changed_rows": changed_count
        }

    return df, mappings_used


def get_stage(row, patterns, lookup_dict):
    """
    Improved version with better substring matching
    """
    
    def get_numeric_part(stage):
        return stage[0] if stage and stage != 'X' else 'X'
    
    def find_best_match(input_value, possible_values):
        """Find the best matching value from possible_values"""
        if pd.isna(input_value):
            return None
            
        # 1. Exact match
        if input_value in possible_values:
            return input_value
            
        # 2. Input is prefix of pattern (e.g., T2 matches T2a, T2b)
        for value in possible_values:
            if value.startswith(input_value):
                return value
                
        # 3. Pattern is prefix of input (e.g., N2 matches N2a, N2b)  
        for value in possible_values:
            if input_value.startswith(value):
                return value
                
        # 4. Contains match as fallback
        for value in possible_values:
            if input_value in value or value in input_value:
                return value
                
        return None

    n_stage = row.get("NStageDer")
    m_stage = row.get("MStageDer")
    t_stage = row.get("TstageDer")

    # Priority 1: M-stage handling
    if pd.notna(m_stage):
        matched_m = find_best_match(m_stage, patterns)
        if matched_m and matched_m.startswith("M"):
            # Find matching N-stage
            n_keys = list(lookup_dict[matched_m].keys())
            matched_n = find_best_match(n_stage, n_keys) if pd.notna(n_stage) else 'N0'
            
            if matched_n:
                stage_value = lookup_dict[matched_m].get(matched_n, lookup_dict[matched_m]['N0'])
                return stage_value if m_stage == matched_m else get_numeric_part(stage_value)

    # Priority 2: T-stage handling
    if pd.notna(t_stage) and pd.notna(n_stage):
        matched_t = find_best_match(t_stage, patterns)
        if matched_t and matched_t.startswith("T"):
            # Find matching N-stage
            n_keys = list(lookup_dict[matched_t].keys())
            matched_n = find_best_match(n_stage, n_keys)
            
            if matched_n:
                stage_value = lookup_dict[matched_t].get(matched_n, 'X')
                if stage_value != 'X':
                    return stage_value if t_stage == matched_t else get_numeric_part(stage_value)

    return 'X'