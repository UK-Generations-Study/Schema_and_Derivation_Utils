import os
import sys
import json
import re
import pandas as pd
from collections import defaultdict
from jsonschema import validate, FormatChecker

sys.path.append(os.path.abspath("N:\\CancerEpidem\\BrBreakthrough\\DeliveryProcess\\Schema_and_Derivation_utils\\Questionnaire\\R0\\scripts"))
import CleaningRules as cr

sys.path.append(os.path.abspath("N:\\CancerEpidem\\BrBreakthrough\\DeliveryProcess\Schema_and_Derivation_utils"))
from utilities import connect_DB, createLogger, read_data

# Shared configuration
def get_config():
    return {
        'Delivery_log_path': 'N:\CancerEpidem\BrBreakthrough\DeliveryProcess\Logs',
        'test_server': 'DoverVTest',
        'r0_json_path': 'N:\\CancerEpidem\\BrBreakthrough\\DeliveryProcess\\Schema_and_Derivation_utils\\Questionnaire\\R0\\json_schemas',
        'out_json_path': 'N:\\CancerEpidem\\BrBreakthrough\\DeliveryProcess\\Data_Output_Testing'
    }

# Data loading and pivoting
def load_and_pivot_data(question_range, logger):
    config = get_config()
    dm_conn = connect_DB('QuestTransformed', config['test_server'], logger)
    
    if question_range == 'BETWEEN 550 AND 739':
        base_query = '''
            SELECT [StudyID], [VariableName], [ResponseText]
            FROM [QuestTransformed].[dbo].[GeneralResponses] 
            WHERE RoundID = 1 AND QuestionID {}
            UNION ALL
            SELECT [StudyID], [VariableName], [ResponseText]
            FROM [QuestTransformed].[dbo].[NestedPregnancies] 
            WHERE RoundID = 1
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
    
    merged = pd.merge(df, dfQuest[['VariableName', 'Section', 'QuestionTypeID']], on='VariableName', how='inner')
    pivoted = pd.pivot(merged, index='StudyID', columns='VariableName', values='ResponseText').fillna('')
    return pivoted, dfPII

# Schema handling
def load_schema(schema_name):
    config = get_config()
    schema_path = os.path.join(config['r0_json_path'], f'{schema_name}.json')
    with open(schema_path, 'r') as f:
        return json.load(f)

# Data processing core
def process_data(raw_data, schema, section_registry=None):
    variable_mapping = {
        prop["name"]: key
        for key, prop in schema["additionalProperties"]["properties"].items()
        if "name" in prop
    }
    
    # Extract constraints
    constraint_map, var_type_map = extract_schema_constraints(schema)
    
    processed = {}
    change_tracking = defaultdict(lambda: defaultdict(lambda: {'count': 0, 'new_value': None}))
    
    for study_id, responses in raw_data.items():
        processed[study_id] = {}
        for var_name, value in responses.items():
            # Handle nested variables
            if section_registry:
                meta = section_registry.rename_variable(var_name)
                field_name = meta['schema_field'] if meta else variable_mapping.get(var_name)
            else:
                field_name = variable_mapping.get(var_name)
            
            # Skip unmapped variables
            if not field_name:
                continue
            
            # Apply cleaning rules
            cleaned_value = clean_value(
                value, 
                var_name, 
                field_name,
                constraint_map.get(field_name, {}),
                var_type_map.get(field_name)
            )
            
            # Track changes
            if str(value) != str(cleaned_value):
                change_tracking[var_name][value]['new_value'] = cleaned_value
                change_tracking[var_name][value]['count'] += 1
            
            processed[study_id][field_name] = cleaned_value
    
    print(f"Processed {len(processed)} participants")
    return processed, change_tracking

# Schema constraint extraction
def extract_schema_constraints(schema):
    constraint_map = {}
    var_type_map = {}
    props = schema["additionalProperties"]["properties"]
    
    for field, config in props.items():
        # Extract type
        field_type = config.get('type', 'string')
        if isinstance(field_type, list):
            field_type = [t for t in field_type if t != 'null'][0]
        var_type_map[field] = field_type
        
        # Extract constraints
        min_val, max_val = None, None
        if 'anyOf' in config:
            for sub in config['anyOf']:
                min_val = sub.get('minimum', min_val)
                max_val = sub.get('maximum', max_val)
        constraint_map[field] = {'min': min_val, 'max': max_val}
        
        # Handle nested properties
        if 'items' in config and 'properties' in config['items']:
            for nested_field, nested_config in config['items']['properties'].items():
                nested_type = nested_config.get('type', 'string')
                if isinstance(nested_type, list):
                    nested_type = [t for t in nested_type if t != 'null'][0]
                var_type_map[nested_field] = nested_type
                
                n_min, n_max = None, None
                if 'anyOf' in nested_config:
                    for sub in nested_config['anyOf']:
                        n_min = sub.get('minimum', n_min)
                        n_max = sub.get('maximum', n_max)
                constraint_map[nested_field] = {'min': n_min, 'max': n_max}
    
    return constraint_map, var_type_map

# Value cleaning
def clean_value(value, var_name, field_name, constraints, expected_type):
    # Apply newValMap conversions
    mapped_value = get_newvalmap_value(var_name, value, field_name)
    if mapped_value is not None:
        return mapped_value
    
    # Handle empty values
    if not value or str(value).strip() in ['', 'null']:
        return None
    
    # Apply type-specific cleaning
    min_val = constraints.get('min')
    max_val = constraints.get('max')
    return cr.rules(value, expected_type, min_val, max_val)

# Value mapping
def get_newvalmap_value(var_name, value, field_name):
    # Check field-specific mappings
    if field_name in cr.newValMap and value in cr.newValMap[field_name]:
        return cr.newValMap[field_name][value]
    
    # Check variable-specific mappings
    if var_name in cr.newValMap and value in cr.newValMap[var_name]:
        return cr.newValMap[var_name][value]
    
    return None

def validate_data(data, schema):
    try:
        validate(instance = data, schema = schema, format_checker = FormatChecker())
        print('JSON data is valid.')
    except ValidationError as e:
        print('Validation Error:', e)

# Save processed data
def save_output(data, output_name, logger):
    config = get_config()
    os.makedirs(config['out_json_path'], exist_ok=True)
    output_path = os.path.join(config['out_json_path'], f'{output_name}.json')
    
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=4)
    
    logger.info(f"Saved output to {output_path}")
    print(f"Output saved: {output_path}")

# PII masking
def mask_pii(data, dfPII):
    pii_mask = dfPII[dfPII['PII'] == 1]
    pii_vars = set(pii_mask['VariableName'])
    
    for study_id in data:
        # Remove top-level PII
        for pii_var in list(data[study_id].keys()):
            if pii_var in pii_vars:
                del data[study_id][pii_var]
        
        # Remove nested PII
        for array_type in ['RecordedHeights', 'Institutions', 'Pregnancies']:
            if array_type in data[study_id]:
                for item in data[study_id][array_type]:
                    for pii_var in list(item.keys()):
                        if pii_var in pii_vars:
                            del item[pii_var]
    
    print("PII masking completed")
    return data