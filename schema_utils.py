# schema_utils.py
import json

def build_variable_mapping(schema):
    """Build mapping from variable names to schema fields"""
    return {
        prop["name"]: key
        for key, prop in schema["additionalProperties"]["properties"].items()
        if "name" in prop
    }

def extract_constraints(schema_props):
    """Extract min/max constraints from schema properties"""
    constraint_map = {}
    for field, props in schema_props.items():
        min_val, max_val = None, None
        
        if "anyOf" in props:
            for subschema in props["anyOf"]:
                if "minimum" in subschema or "maximum" in subschema:
                    min_val = subschema.get("minimum")
                    max_val = subschema.get("maximum")
                    break
        
        if min_val is None and "minimum" in props:
            min_val = props["minimum"]
        if max_val is None and "maximum" in props:
            max_val = props["maximum"]
        
        constraint_map[field] = {"min": min_val, "max": max_val}
        
        # Handle nested items
        if "items" in props and "properties" in props["items"]:
            for nested_field, nested_props in props["items"]["properties"].items():
                nested_min, nested_max = None, None
                
                if "anyOf" in nested_props:
                    for subschema in nested_props["anyOf"]:
                        if "minimum" in subschema or "maximum" in subschema:
                            nested_min = subschema.get("minimum")
                            nested_max = subschema.get("maximum")
                            break
                
                if nested_min is None and "minimum" in nested_props:
                    nested_min = nested_props["minimum"]
                if nested_max is None and "maximum" in nested_props:
                    nested_max = nested_props["maximum"]
                
                constraint_map[nested_field] = {"min": nested_min, "max": nested_max}
    
    print("Extracted constraints for all fields")
    return constraint_map

def extract_var_types(schema_props):
    """Extract variable types from schema properties"""
    var_type_map = {}
    for field, props in schema_props.items():
        if "type" in props:
            field_type = props["type"]
            if isinstance(field_type, list):
                field_type = [t for t in field_type if t != "null"]
                if field_type:
                    var_type_map[field] = field_type[0]
            else:
                var_type_map[field] = field_type
        
        # Handle nested items
        if "items" in props and "properties" in props["items"]:
            for nested_field, nested_props in props["items"]["properties"].items():
                if "type" in nested_props:
                    nested_type = nested_props["type"]
                    if isinstance(nested_type, list):
                        nested_type = [t for t in nested_type if t != "null"]
                        if nested_type:
                            var_type_map[nested_field] = nested_type[0]
                    else:
                        var_type_map[nested_field] = nested_type
    
    print("Extracted variable types for all fields")
    return var_type_map