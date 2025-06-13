from common_utils import *

def process_flat_section(schema_file, section_name, question_range):
    """Process flat/non-nested questionnaire sections"""
    logger = createLogger(section_name, Delivery_log_path)
    
    # Build queries
    base_query = f"SELECT * FROM [QuestTransformed].[dbo].[GeneralResponses] WHERE RoundID = 1 AND {question_range}"
    quest_query = f"SELECT * FROM [QuestTransformed].[dbo].[Questions] WHERE RoundID = 1 AND {question_range}"
    pii_query = f"SELECT * FROM [QuestTransformed].[dbo].[VariableFlagging] WHERE {question_range}"
    
    # Load data
    df = load_data(logger, 'QuestTransformed', base_query)
    dfQuest = load_data(logger, 'QuestTransformed', quest_query)
    dfPII = load_data(logger, 'QuestTransformed', pii_query)
    pivotedDict = pivot_data(df, dfQuest)
    
    # Load schema
    with open(os.path.join(r0_json_path, schema_file), 'r') as f:
        schema = json.load(f)
    variable_mapping = build_variable_mapping(schema)
    
    # Process data
    processed_data, _ = process_flat_data(pivotedDict, schema, variable_mapping)
    
    # Validate and save
    validate(instance=processed_data, schema=schema, format_checker=FormatChecker())
    output_file = f'Output_{section_name}.json'
    save_output(processed_data, out_json_path, output_file, logger)
    
    # Remove PII
    pii_vars = dfPII[dfPII['PII'] == 1]['VariableName'].tolist()
    for participant in processed_data.values():
        for pii_var in pii_vars:
            if pii_var in variable_mapping and variable_mapping[pii_var] in participant:
                del participant[variable_mapping[pii_var]]
    
    return processed_data

def convert_flat_value(value, field_schema, field_name):
    """Value conversion logic for flat sections"""
    if field_name in cr.newValMap and value in cr.newValMap[field_name]:
        return cr.newValMap[field_name][value]
    
    if not value or value.lower() in ["null", "na", ""]:
        return None
    
    # Type conversion and range checking
    expected_type = field_schema.get("type", "string")
    cleaned_value = cr.rules(value, expected_type)
    
    # Handle special codes
    if isinstance(cleaned_value, (int, float)) and cleaned_value in {77777, 88888, 99999}:
        return cleaned_value
    
    # Range validation
    min_val, max_val = None, None
    if "anyOf" in field_schema:
        for subschema in field_schema["anyOf"]:
            min_val = subschema.get("minimum", min_val)
            max_val = subschema.get("maximum", max_val)
    
    try:
        if expected_type == "integer":
            converted = int(cleaned_value)
            if (min_val and converted < min_val) or (max_val and converted > max_val):
                return 77777
            return converted
        # Add other type handling as needed
    except (ValueError, TypeError):
        return None
    return cleaned_value

def process_flat_data(data, schema, variable_mapping):
    """Core processing logic for flat sections"""
    processed_data = {}
    change_tracking = defaultdict(lambda: defaultdict(lambda: {'count': 0, 'new_value': None}))
    
    for study_id, responses in data.items():
        processed_data[study_id] = {}
        for var_name, value in responses.items():
            json_key = variable_mapping.get(var_name)
            if not json_key:
                continue
                
            field_schema = schema["additionalProperties"]["properties"][json_key]
            new_value = convert_flat_value(value, field_schema, json_key)
            
            # Track changes
            if str(value) != str(new_value):
                change_tracking[var_name][value]['new_value'] = new_value
                change_tracking[var_name][value]['count'] += 1
                
            processed_data[study_id][json_key] = new_value
            
    return processed_data, change_tracking