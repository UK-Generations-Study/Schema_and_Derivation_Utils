import NestedVariables as nv

def restructure_physical_dev(processed_data, physdev_schema, variable_mapping):
    """Restructures data with numbering variables for array entries"""
    restructured = {}
    
    for participant_id, data in processed_data.items():
        # Initialize JSON structure with numbering variables
        json_data = {
            'RecordedHeights': [],
            'Institutions': []
        }
        
        # Process each variable
        for var_name, value in data.items():
            meta = nv.rename_variable(var_name)
            
            # Handle top-level variables
            if meta is None:
                if var_name in variable_mapping:
                    schema_key = variable_mapping[var_name]
                    json_data[schema_key] = value
                continue
            
            # Handle nested structures
            if meta['section'] == 'physical_dev':
                entry_idx = meta['entry_num'] - 1 if meta['entry_num'] is not None else None
                schema_field = meta['schema_field']
                
                # Determine array type
                if schema_field.startswith('R0_RecHght'):
                    array_name = 'RecordedHeights'
                    max_items = 3
                    num_field = 'R0_RecHeight_Num'  # Numbering variable
                elif (schema_field.startswith('R0_Institution') or 
                      schema_field.startswith('R0_AgeMeas')):
                    array_name = 'Institutions'
                    max_items = 2
                    num_field = 'R0_Inst_Num'  # Numbering variable
                else:
                    continue
                
                # Skip invalid entries
                if entry_idx is None or entry_idx < 0 or entry_idx >= max_items:
                    continue
                
                # Ensure array size
                while len(json_data[array_name]) <= entry_idx:
                    # Create new entry with numbering variable
                    new_entry = {num_field: len(json_data[array_name]) + 1}
                    json_data[array_name].append(new_entry)
                
                # Store value
                json_data[array_name][entry_idx][schema_field] = value
        
        # Clean empty array entries
        json_data['RecordedHeights'] = [e for e in json_data['RecordedHeights'] 
                                       if any(v is not None for k, v in e.items() if k != 'R0_RecHeight_Num')]
        json_data['Institutions'] = [e for e in json_data['Institutions'] 
                                    if any(v is not None for k, v in e.items() if k != 'R0_Inst_Num')]
        
        restructured[participant_id] = json_data
    
    return restructured

def restructure_pregnancies(processed_data, preg_schema, variable_mapping):
    """Restructures data with numbering variables for array entries"""
    restructured = {}
    
    for participant_id, data in processed_data.items():
        # Initialize JSON structure
        json_data = {'Pregnancies': []}
        preg_dict = {}  # {pregnancy_num: {field: value}}
        
        # Process each variable
        for var_name, value in data.items():
            meta = nv.rename_variable(var_name)
            
            # Handle top-level variables
            if meta is None:
                if var_name in variable_mapping:
                    schema_key = variable_mapping[var_name]
                    json_data[schema_key] = value
                continue
            
            # Handle pregnancy variables
            if meta['section'] == 'pregnancies':
                preg_num = meta['entry_num']
                schema_field = meta['schema_field']
                
                # Initialize pregnancy entry
                if preg_num not in preg_dict:
                    preg_dict[preg_num] = {'R0_PregNum': preg_num}
                
                # Store value
                preg_dict[preg_num][schema_field] = value
        
        # Convert pregnancy dictionary to sorted list
        for preg_num in sorted(preg_dict.keys()):
            json_data['Pregnancies'].append(preg_dict[preg_num])
        
        # Clean empty pregnancy entries
        json_data['Pregnancies'] = [p for p in json_data['Pregnancies'] 
                                  if any(v is not None for k, v in p.items() if k != 'R0_PregNum')]
        
        restructured[participant_id] = json_data
    
    return restructured