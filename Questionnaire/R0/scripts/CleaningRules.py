import re

# Predefined value mappings (Critical for categorical conversions)
newValMap = {
    "R0_EthnGroupWhite": {"A": 12}, 
    "R0_EthnGroupJAshk": {"D": 15, "1": 88888}, 
    "R0_EthnGroupBlkCarib": {"0": 88888},
    "R0_EthnGroupBlkAfr": {"0": 88888},
    "R0_EthnGroupNone": {"B": 13, "C": 14},
    "R0_EthnGroupBlkOther": {"0": 88888},
    "R0_Surname": {15: "15"},
    "R0_MatPregWks": {"FT": 40},
    "R0_InfertilityPeriod": {"2": 2},
    "R0_Preg_Outcome": {"A": 10, "B": 11, "H": 12, "M": 13, "Z": 14},
    "R0_RecHght_Yr": {'2012': 2012}
}

def rules(value, expected_type, min_val=None, max_val=None):
    """
    Enhanced cleaning logic with:
    1. Automatic conversion of invalid strings to 88888 for numeric fields
    2. Special handling for "B" values in weight comparison fields
    3. Convert special codes to descriptive strings for string fields
    """
    # Step 1: Attempt type conversion
    original_value = value
    if isinstance(value, str) and expected_type in ("integer", "float"):
        stripped = value.strip()
        
        # NEW: Handle leading zeros for integers
        if expected_type == "integer" and stripped.isdigit():
            # Remove leading zeros and convert to int
            value = int(stripped.lstrip('0') if stripped.lstrip('0') != '' else 0)
        else:
            try:
                if expected_type == "integer":
                    value = int(stripped)
                else:  # float
                    value = float(stripped)
            except ValueError:
                # Keep as string for now, will handle below
                pass

    # Step 2: Handle special codes and blanks
    if isinstance(value, str) and value in ("NA", "NK", "DK", "UK", "KN", "00NK", "00DK"):
        if expected_type == "string":
            value = "Unknown - Entry Unknown"
        else:
            value = 99999 if expected_type == "integer" else 99999.0
    elif isinstance(value, str) and value.strip() == '':
        value = None

    # Step 3: Check for invalid patterns
    elif isinstance(value, str):
        stripped_value = value.strip()
        if stripped_value and re.fullmatch(r'^(\d+[\W]|[\W]\d+|[\W]{1,2}|\dX)$', stripped_value):
            if expected_type == "string":
                value = "Unknown - Invalid Entry"
            else:
                value = 88888 if expected_type == "integer" else 88888.0
                
    # NEW STEP: Convert any remaining strings to 88888 for numeric fields
    if isinstance(value, str) and expected_type in ("integer", "float"):
        value = 88888 if expected_type == "integer" else 88888.0

    # Step 4: Apply min/max constraints
    if isinstance(value, (int, float)) and value not in {77777, 88888, 99999}:
        if min_val is not None and value < min_val:
            value = 77777 if expected_type == "integer" else 77777.0
        elif max_val is not None and value > max_val:
            value = 77777 if expected_type == "integer" else 77777.0

    # NEW STEP: Convert special codes to descriptive strings for string fields
    if expected_type == "string" and value in (77777, 88888, 99999, 77777.0, 88888.0, 99999.0):
        if value in (77777, 77777.0):
            value = "Unknown - Unlikely or impossible answer"
        elif value in (88888, 88888.0):
            value = "Unknown - Invalid Entry"
        elif value in (99999, 99999.0):
            value = "Unknown - Entry Unknown"

    return value