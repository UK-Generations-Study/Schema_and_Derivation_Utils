# processing_utils.py
import re
from CleaningRules import rules
from NestedVariables import rename_variable

# processing_utils.py (updated get_newvalmap_value)
def get_newvalmap_value(var_name, value, field_name=None, newValMap=None):
    """Enhanced newValMap lookup with fallback logic"""
    if newValMap is None:
        return None
        
    # 1. Try direct field_name mapping
    if field_name and field_name in newValMap and value in newValMap[field_name]:
        return newValMap[field_name][value]
    
    # 2. Try direct var_name mapping
    if var_name in newValMap and value in newValMap[var_name]:
        return newValMap[var_name][value]
    
    return None

# processing_utils.py (updated)
def convert_value(value, var_name, field_name, var_type_map, constraint_map, newValMap):
    """
    Enhanced conversion with proper type handling
    """
    # 1. SPECIAL CASE: Bra cup size other
    if field_name in ["R0_BraCupSize_Other", "R0_BraCupSize_20Other"]:
        if value is None or (isinstance(value, str) and value.strip() == ""):
            return None
        cleaned = str(value).strip().upper()
        pat_digits_then_letters = r"^\d{2}[A-Z]{1,2}$"
        pat_letters_only = r"^[A-Z]{1,3}$"
        if re.fullmatch(pat_digits_then_letters, cleaned) or re.fullmatch(pat_letters_only, cleaned):
            return cleaned
        return "Unknown - Invalid Entry"

    # 2. newValMap lookup
    mapped_value = get_newvalmap_value(var_name, value, field_name, newValMap)
    if mapped_value is not None:
        return mapped_value
    
    # 3. Get expected type
    expected_type = 'string'  # Default
    if field_name and field_name in var_type_map:
        expected_type = var_type_map[field_name]
    elif var_name in var_type_map:
        expected_type = var_type_map[var_name]
    
    # 4. Get min/max constraints
    min_val, max_val = None, None
    if field_name and field_name in constraint_map:
        min_val = constraint_map[field_name]["min"]
        max_val = constraint_map[field_name]["max"]
    elif var_name in constraint_map:
        min_val = constraint_map[var_name]["min"]
        max_val = constraint_map[var_name]["max"]
    
    # 5. Use CleaningRules
    cleaned_value = rules(value, expected_type, min_val, max_val)

    # Special handling for array variables - only if field_name exists
    if field_name and field_name.startswith(('R0_RecHght', 'R0_Institution', 'R0_AgeMeas')) and \
       expected_type == 'integer' and \
       isinstance(cleaned_value, str) and \
       cleaned_value.isdigit():
        return int(cleaned_value)

    return cleaned_value

def process_data(raw_data, variable_mapping, var_type_map, constraint_map, newValMap):
    """Processes data while maintaining original variable names - FIXED STRUCTURE"""
    processed_data = {}
    change_tracking = {}

    for participant_id, responses in raw_data.items():
        participant_data = {}
        for var_name, raw_value in responses.items():
            # ... [existing processing logic] ...
            participant_data[var_name] = cleaned_value

        # Store participant data with StudyID as key
        processed_data[participant_id] = participant_data

    return processed_data, change_tracking

def process_data(raw_data, variable_mapping, var_type_map, constraint_map, newValMap):
    """Processes data while maintaining original variable names"""
    processed_data = {}
    change_tracking = {}

    for participant_id, responses in raw_data.items():
        participant_data = {}
        for var_name, raw_value in responses.items():
            # Look up which JSON field this SQL var maps to
            meta = rename_variable(var_name)
            field_name = meta['schema_field'] if meta else variable_mapping.get(var_name)
            
            cleaned_value = convert_value(
                raw_value, 
                var_name, 
                field_name,
                var_type_map,
                constraint_map,
                newValMap
            )

            # Track changes
            if str(raw_value) != str(cleaned_value):
                if var_name not in change_tracking:
                    change_tracking[var_name] = {}
                if raw_value not in change_tracking[var_name]:
                    change_tracking[var_name][raw_value] = {
                        'new_value': cleaned_value,
                        'count': 0
                    }
                change_tracking[var_name][raw_value]['count'] += 1

            participant_data[var_name] = cleaned_value

        processed_data[participant_id] = participant_data

    print(f"Processed data for {len(processed_data)} participants")
    return processed_data, change_tracking

def save_change_tracking(change_tracking, file_path):
    """Save change tracking to JSON file"""
    with open(file_path, 'w') as f:
        json.dump(change_tracking, f, indent=2)
    print(f"Saved change tracking to {file_path}")