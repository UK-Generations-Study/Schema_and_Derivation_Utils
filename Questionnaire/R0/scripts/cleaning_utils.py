"""
Cleaning utilities for questionnaire ETL.

This module provides:
- `newValMap`: hard-coded mappings from raw codes to cleaned numeric values
  for a small set of questionnaire variables.
- `rules`: a generic cleaner that applies type conversion and min/max bounds
  based on JSON schema expectations.
- `convert_to_date`: a thin wrapper around `pd.to_datetime` for date conversion.

These helpers are used by higher-level ETL scripts (e.g. `common_utils`) to
standardise raw database values into consistent, analysable forms.
"""

import re
import pandas as pd

# Predefined value mappings (Critical for categorical conversions)
newValMap = {
    "R0_EthnGroupWhite": {"A": 12}, 
    "R0_EthnGroupJAshk": {"D": 15}, 
    "R0_EthnGroupNone": {"B": 13, "C": 14},
    "R0_MatPregWks": {"FT": 40},
    "R0_Preg_Outcome": {"A": 10, "B": 11, "H": 12, "M": 13, "Z": 14},
    "R0_Preg_DurationWks": {"FT": 40},
    "R0_ThyroidDiseaseType": {"A": 5},
    "R0_EatingDisorder": {"A": 5},
    "R0_ED_GainLoss": {"A": 4}
}

# Updated rules function
def rules(value, expected_type, min_val=None, max_val=None):
    # Step 1: Handle null/empty values
    if value is None or (isinstance(value, str) and value.strip() in ('', 'null')):
        return None

    # Step 2: Handle special codes
    if isinstance(value, str) and value in ("NA", "NK", "DK", "UK", "KN", "00NK", "00DK"):
        return None

    # Step 3: Attempt type conversion
    if expected_type == "integer":
        stripped = str(value).strip()
        try:
            if stripped in ('', 'null'):
                return None
            # Handle floats that represent integers
            if '.' in stripped:
                num = float(stripped)
                if num.is_integer():
                    cleaned_value = int(num)
                else:
                    return None  # Not an integer
            else:
                cleaned_value = int(stripped)
        except (ValueError, TypeError):
            return None

        # Apply min/max constraints
        if min_val is not None and cleaned_value < min_val:
            return None
        if max_val is not None and cleaned_value > max_val:
            return None
            
        return cleaned_value

    return value  # Default return for non-integer types

def convert_to_date(value):
    """Convert cleaned values to pandas datetime objects"""
    if value is None:
        return None
    try:
        return pd.to_datetime(value, errors='coerce')
    except:
        return None