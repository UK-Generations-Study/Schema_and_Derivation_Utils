import re

# Define a list of tuples that map regex patterns (with a capturing group for the pregnancy number)
# to the corresponding schema field.
#
# Note:
# • For some fields the numeric pregnancy number is taken from the capturing group.
# • Some columns (e.g., for Outcome or MilkSuppression) might not need an explicit capture;
#   in that case we extract the number from the end of the column name.

# SECTION_REGISTRY maps variable patterns to schema fields and provides reverse mappings.
# This central configuration ensures consistent variable renaming across sections (physical_dev, pregnancies).
# - 'patterns': Regex patterns to extract schema fields and entry numbers from variable names.
# - 'reverse_map': Functions to reconstruct original variable names from schema fields and entry numbers.
section_registry = {
    'physical_dev': {
        'patterns': {
            'recheights': [
             # Matches variables like "Q3_14day1" to "RecHght_Day" with entry_num=1
            (r'^Q3_14day(\d+)$', 'R0_RecHght_Day'),
            (r'^Q3_14month(\d+)$', 'R0_RecHght_Mnth'),
            (r'^Q3_14year(\d+)$', 'R0_RecHght_Yr'),
            (r'^Q3_14ageY(\d+)$', 'R0_RecHght_AgeYr'),
            (r'^Q3_14ageM(\d+)$', 'R0_RecHght_AgeMnth'),
            (r'^Q3_14FT(\d+)$', 'R0_RecHght_Ft'),
            (r'^Q3_14IN(\d+)$', 'R0_RecHght_In'),
            (r'^Q3_14CM(\d+)$', 'R0_RecHght_Cm')
            ],
            'institutions': [
            # Institutions 1-2
            (r'^Q3_15inst(\d+)$', 'R0_InstitutionName'),
            (r'^Q3_15town(\d+)$', 'R0_InstitutionTown'),
            (r'^Q3_15age(\d+)$', 'R0_AgeMeas'),
            (r'^Q3_15from(\d+)$', 'R0_AgeMeas_From'),
            (r'^Q3_15to(\d+)$', 'R0_AgeMeas_To')
            ]
        },
        'reverse_map': {
            # Lambda functions rebuild original variable names (e.g., "RecHght_Day" → "Q3_14day{num}")
            "R0_RecHght_Day": lambda num: f"Q3_14day{num}",
            "R0_RecHght_Mnth": lambda num: f"Q3_14month{num}",
            "R0_RecHght_Yr": lambda num: f"Q3_14year{num}",
            "R0_RecHght_AgeYr": lambda num: f"Q3_14ageY{num}",
            "R0_RecHght_AgeMnth": lambda num: f"Q3_14ageM{num}",
            "R0_RecHght_Ft": lambda num: f"Q3_14FT{num}",
            "R0_RecHght_In": lambda num: f"Q3_14IN{num}",
            "R0_RecHght_Cm": lambda num: f"Q3_14CM{num}",
            
            # Institutions
            "R0_InstitutionName": lambda num: f"Q3_15inst{num}",
            "R0_InstitutionTown": lambda num: f"Q3_15town{num}",
            "R0_AgeMeas": lambda num: f"Q3_15age{num}",
            "R0_AgeMeas_From": lambda num: f"Q3_15from{num}",
            "R0_AgeMeas_To": lambda num: f"Q3_15to{num+1}"
        }
    },
    'pregnancies': {
        'patterns': {
            'pregnancies': [
            # Captures pregnancy-related variables across different numbering schemes (1-3, 4-6, 7-13)
            # Pattern for pregnancies 1-3 (using Q5_4_ and Q5_5_ etc.)
            (r'^Q5_4_D_(\d+)$', 'R0_Preg_EndDay'),
            (r'^Q5_4_M_(\d+)$', 'R0_Preg_EndMnth'),
            (r'^Q5_4_Y_(\d+)$', 'R0_Preg_EndYr'),
            (r'^Q5_5_(\d+)_1$', 'R0_Preg_Outcome'),
            (r'^Q5_6_(\d+)$', 'R0_Preg_DurationWks'),
            (r'^Q5_7_(\d+)_1$', 'R0_Preg_SevereVomiting'),
            (r'^Q5_8_(\d+)_1$', 'R0_Preg_Eclampsia'),
            (r'^Q5_9_(\d+)_1$', 'R0_Preg_ChildSex'),
            (r'^Q5_10_GR_(\d+)$', 'R0_Preg_BirthWghtG'),
            (r'^Q5_10_LB_(\d+)$', 'R0_Preg_BirthWghtlbs'),
            (r'^Q5_10_OZ_(\d+)$', 'R0_Preg_BirthWghtOzs'),
            (r'^Q5_11_(\d+)$', 'R0_Preg_BreastfeedingWks'),
            (r'^Q5_12_(\d+)_1$', 'R0_Preg_MilkSuppression'),
            # Pattern for pregnancies 4-6 (columns now use Q5_13_ and Q5_14_ etc.)
            (r'^Q5_13_D_(\d+)$', 'R0_Preg_EndDay'),
            (r'^Q5_13_M_(\d+)$', 'R0_Preg_EndMnth'),
            (r'^Q5_13_Y_(\d+)$', 'R0_Preg_EndYr'),
            (r'^Q5_14_(\d+)_1$', 'R0_Preg_Outcome'),
            (r'^Q5_15_(\d+)$', 'R0_Preg_DurationWks'),
            (r'^Q5_16_(\d+)_1$', 'R0_Preg_SevereVomiting'),
            (r'^Q5_17_(\d+)_1$', 'R0_Preg_Eclampsia'),
            (r'^Q5_18_(\d+)_1$', 'R0_Preg_ChildSex'),
            (r'^Q5_19_GR_(\d+)$', 'R0_Preg_BirthWghtG'),
            (r'^Q5_19_LB_(\d+)$', 'R0_Preg_BirthWghtlbs'),
            (r'^Q5_19_OZ_(\d+)$', 'R0_Preg_BirthWghtOzs'),
            (r'^Q5_20_(\d+)$', 'R0_Preg_BreastfeedingWks'),
            (r'^Q5_21_(\d+)_1$', 'R0_Preg_MilkSuppression'),
            # Pattern for pregnancies 7-13 (these use Gen07_full@ as a prefix)
            (r'^Gen07_full@Q5_13_D_(\d+)$', 'R0_Preg_EndDay'),
            (r'^Gen07_full@Q5_13_M_(\d+)$', 'R0_Preg_EndMnth'),
            (r'^Gen07_full@Q5_13_Y_(\d+)$', 'R0_Preg_EndYr'),
            (r'^Gen07_full@Q5_14_(\d+)_1$', 'R0_Preg_Outcome'),
            (r'^Gen07_full@Q5_15_(\d+)$', 'R0_Preg_DurationWks'),
            (r'^Gen07_full@Q5_16_(\d+)_1$', 'R0_Preg_SevereVomiting'),
            (r'^Gen07_full@Q5_17_(\d+)_1$', 'R0_Preg_Eclampsia'),
            (r'^Gen07_full@Q5_18_(\d+)_1$', 'R0_Preg_ChildSex'),
            (r'^Gen07_full@Q5_19_GR_(\d+)$', 'R0_Preg_BirthWghtG'),
            (r'^Gen07_full@Q5_19_LB_(\d+)$', 'R0_Preg_BirthWghtlbs'),
            (r'^Gen07_full@Q5_19_OZ_(\d+)$', 'R0_Preg_BirthWghtOzs'),
            (r'^Gen07_full@Q5_20_(\d+)$', 'R0_Preg_BreastfeedingWks'),
            (r'^Gen07_full@Q5_21_(\d+)_1$', 'R0_Preg_MilkSuppression')
            ]
        },
        'reverse_map': {
            # Nested mappings handle different variable prefixes based on pregnancy number thresholds
            "R0_Preg_EndDay": {
                # Pregnancies 1-3: Q5_4_D_*
                1: lambda num: f"Q5_4_D_{num}",
                # Pregnancies 4-6: Q5_13_D_*
                4: lambda num: f"Q5_13_D_{num}",
                # Pregnancies 7-13: Gen07_full@Q5_13_D_*
                7: lambda num: f"Gen07_full@Q5_13_D_{num}"
            },
            "R0_Preg_EndMnth": {
                1: lambda num: f"Q5_4_M_{num}",
                4: lambda num: f"Q5_13_M_{num}",
                7: lambda num: f"Gen07_full@Q5_13_M_{num}"
            },
            "R0_Preg_EndYr": {
                1: lambda num: f"Q5_4_Y_{num}",
                4: lambda num: f"Q5_13_Y_{num}",
                7: lambda num: f"Gen07_full@Q5_13_Y_{num}"
            },
            "R0_Preg_Outcome": {
                1: lambda num: f"Q5_5_{num}_1",
                4: lambda num: f"Q5_14_{num}_1",
                7: lambda num: f"Gen07_full@Q5_14_{num}_1"
            },
            "R0_Preg_DurationWks": {
                1: lambda num: f"Q5_6_{num}",
                4: lambda num: f"Q5_15_{num}",
                7: lambda num: f"Gen07_full@Q5_15_{num}"
            },
            "R0_Preg_SevereVomiting": {
                1: lambda num: f"Q5_7_{num}_1",
                4: lambda num: f"Q5_16_{num}_1",
                7: lambda num: f"Gen07_full@Q5_16_{num}_1"
            },
            "R0_Preg_Eclampsia": {
                1: lambda num: f"Q5_8_{num}_1",
                4: lambda num: f"Q5_17_{num}_1",
                7: lambda num: f"Gen07_full@Q5_17_{num}_1"
            },
            "R0_Preg_ChildSex": {
                1: lambda num: f"Q5_9_{num}_1",
                4: lambda num: f"Q5_18_{num}_1",
                7: lambda num: f"Gen07_full@Q5_18_{num}_1"
            },
            "R0_Preg_BirthWghtG": {
                1: lambda num: f"Q5_10_GR_{num}",
                4: lambda num: f"Q5_19_GR_{num}",
                7: lambda num: f"Gen07_full@Q5_19_GR_{num}"
            },
            "R0_Preg_BirthWghtlbs": {
                1: lambda num: f"Q5_10_LB_{num}",
                4: lambda num: f"Q5_19_LB_{num}",
                7: lambda num: f"Gen07_full@Q5_19_LB_{num}"
            },
            "R0_Preg_BirthWghtOzs": {
                1: lambda num: f"Q5_10_OZ_{num}",
                4: lambda num: f"Q5_19_OZ_{num}",
                7: lambda num: f"Gen07_full@Q5_19_OZ_{num}"
            },
            "R0_Preg_BreastfeedingWks": {
                1: lambda num: f"Q5_11_{num}",
                4: lambda num: f"Q5_20_{num}",
                7: lambda num: f"Gen07_full@Q5_20_{num}"
            },
            "R0_Preg_MilkSuppression": {
                1: lambda num: f"Q5_12_{num}_1",
                4: lambda num: f"Q5_21_{num}_1",
                7: lambda num: f"Gen07_full@Q5_21_{num}_1"
            }
        }
    }
}

def rename_variable(var_name):
    """Matches a variable name to its schema field using regex patterns in SECTION_REGISTRY.
    Returns metadata including the section, schema field, entry number, and original name.
    This is critical for transforming raw SQL variables into structured JSON fields.
    """
    """Handles special numbering for AgeMeas_To variables"""
    for section, config in section_registry.items():
        for subgroup in config['patterns'].values():
            for pattern, schema_field in subgroup:
                match = re.match(pattern, str(var_name))
                if match:
                    entry_num = int(match.group(1)) if match.lastindex else None
                    
                    # Special handling for AgeMeas_To variables
                    if schema_field == 'R0_AgeMeas_To':
                        # Shift numbering: 2→1, 3→2
                        if entry_num in (2, 3):
                            entry_num -= 1
                        else:
                            # Skip invalid entries (1, 4+)
                            continue
                    
                    return {
                        'section': section,
                        'schema_field': schema_field,
                        'entry_num': entry_num,
                        'original': var_name
                    }
    return None

# For demonstration, build a lookup table using the provided column names:
pregColumns = [
    "Q5_4_D_1", "Q5_4_M_1", "Q5_4_Y_1", "Q5_5_1_1", "Q5_6_1", "Q5_7_1_1", "Q5_8_1_1", "Q5_9_1_1",
    "Q5_10_GR_1", "Q5_10_LB_1", "Q5_10_OZ_1", "Q5_11_1", "Q5_12_1_1",
    "Q5_4_D_2", "Q5_4_M_2", "Q5_4_Y_2", "Q5_5_2_1", "Q5_6_2", "Q5_7_2_1", "Q5_8_2_1", "Q5_9_2_1",
    "Q5_10_GR_2", "Q5_10_LB_2", "Q5_10_OZ_2", "Q5_11_2", "Q5_12_2_1",
    "Q5_4_D_3", "Q5_4_M_3", "Q5_4_Y_3", "Q5_5_3_1", "Q5_6_3", "Q5_7_3_1", "Q5_8_3_1", "Q5_9_3_1",
    "Q5_10_GR_3", "Q5_10_LB_3", "Q5_10_OZ_3", "Q5_11_3", "Q5_12_3_1",
    "Q5_13_D_4", "Q5_13_M_4", "Q5_13_Y_4", "Q5_14_4_1", "Q5_15_4", "Q5_16_4_1", "Q5_17_4_1", "Q5_18_4_1",
    "Q5_19_GR_4", "Q5_19_LB_4", "Q5_19_OZ_4", "Q5_20_4", "Q5_21_4_1",
    "Q5_13_D_5", "Q5_13_M_5", "Q5_13_Y_5", "Q5_14_5_1", "Q5_15_5", "Q5_16_5_1", "Q5_17_5_1", "Q5_18_5_1",
    "Q5_19_GR_5", "Q5_19_LB_5", "Q5_19_OZ_5", "Q5_20_5", "Q5_21_5_1",
    "Q5_13_D_6", "Q5_13_M_6", "Q5_13_Y_6", "Q5_14_6_1", "Q5_15_6", "Q5_16_6_1", "Q5_17_6_1", "Q5_18_6_1",
    "Q5_19_GR_6", "Q5_19_LB_6", "Q5_19_OZ_6", "Q5_20_6", "Q5_21_6_1",
    "Gen07_full@Q5_13_D_7", "Gen07_full@Q5_13_M_7", "Gen07_full@Q5_13_Y_7", "Gen07_full@Q5_14_7_1", "Gen07_full@Q5_15_7",
    "Gen07_full@Q5_16_7_1", "Gen07_full@Q5_17_7_1", "Gen07_full@Q5_18_7_1",
    "Gen07_full@Q5_19_GR_7", "Gen07_full@Q5_19_LB_7", "Gen07_full@Q5_19_OZ_7", "Gen07_full@Q5_20_7", "Gen07_full@Q5_21_7_1",
    "Gen07_full@Q5_13_D_8", "Gen07_full@Q5_13_M_8", "Gen07_full@Q5_13_Y_8", "Gen07_full@Q5_14_8_1", "Gen07_full@Q5_15_8",
    "Gen07_full@Q5_16_8_1", "Gen07_full@Q5_17_8_1", "Gen07_full@Q5_18_8_1",
    "Gen07_full@Q5_19_GR_8", "Gen07_full@Q5_19_LB_8", "Gen07_full@Q5_19_OZ_8", "Gen07_full@Q5_20_8", "Gen07_full@Q5_21_8_1",
    "Gen07_full@Q5_13_D_9", "Gen07_full@Q5_13_M_9", "Gen07_full@Q5_13_Y_9", "Gen07_full@Q5_14_9_1", "Gen07_full@Q5_15_9",
    "Gen07_full@Q5_16_9_1", "Gen07_full@Q5_17_9_1", "Gen07_full@Q5_18_9_1",
    "Gen07_full@Q5_19_GR_9", "Gen07_full@Q5_19_LB_9", "Gen07_full@Q5_19_OZ_9", "Gen07_full@Q5_20_9", "Gen07_full@Q5_21_9_1",
    "Gen07_full@Q5_13_D_10", "Gen07_full@Q5_13_M_10", "Gen07_full@Q5_13_Y_10", "Gen07_full@Q5_14_10_1", "Gen07_full@Q5_15_10",
    "Gen07_full@Q5_16_10_1", "Gen07_full@Q5_17_10_1", "Gen07_full@Q5_18_10_1",
    "Gen07_full@Q5_19_GR_10", "Gen07_full@Q5_19_LB_10", "Gen07_full@Q5_19_OZ_10", "Gen07_full@Q5_20_10", "Gen07_full@Q5_21_10_1",
    "Gen07_full@Q5_13_D_11", "Gen07_full@Q5_13_M_11", "Gen07_full@Q5_13_Y_11", "Gen07_full@Q5_14_11_1", "Gen07_full@Q5_15_11",
    "Gen07_full@Q5_16_11_1", "Gen07_full@Q5_17_11_1", "Gen07_full@Q5_18_11_1",
    "Gen07_full@Q5_19_GR_11", "Gen07_full@Q5_19_LB_11", "Gen07_full@Q5_19_OZ_11", "Gen07_full@Q5_20_11", "Gen07_full@Q5_21_11_1",
    "Gen07_full@Q5_13_D_12", "Gen07_full@Q5_13_M_12", "Gen07_full@Q5_13_Y_12", "Gen07_full@Q5_14_12_1", "Gen07_full@Q5_15_12",
    "Gen07_full@Q5_16_12_1", "Gen07_full@Q5_17_12_1", "Gen07_full@Q5_18_12_1",
    "Gen07_full@Q5_19_GR_12", "Gen07_full@Q5_19_LB_12", "Gen07_full@Q5_19_OZ_12", "Gen07_full@Q5_20_12", "Gen07_full@Q5_21_12_1",
    "Gen07_full@Q5_13_D_13", "Gen07_full@Q5_13_M_13", "Gen07_full@Q5_13_Y_13", "Gen07_full@Q5_14_13_1", "Gen07_full@Q5_15_13",
    "Gen07_full@Q5_16_13_1", "Gen07_full@Q5_17_13_1", "Gen07_full@Q5_18_13_1",
    "Gen07_full@Q5_19_GR_13", "Gen07_full@Q5_19_LB_13", "Gen07_full@Q5_19_OZ_13", "Gen07_full@Q5_20_13", "Gen07_full@Q5_21_13_1"
]

# Physical Development Columns Equivalent --------------------------------------
physdevColumns = [
    # Recorded Heights 1-3
    "Q3_14day1", "Q3_14month1", "Q3_14year1", "Q3_14ageY1", "Q3_14ageM1",
    "Q3_14FT1", "Q3_14IN1", "Q3_14CM1",
    "Q3_14day2", "Q3_14month2", "Q3_14year2", "Q3_14ageY2", "Q3_14ageM2",
    "Q3_14FT2", "Q3_14IN2", "Q3_14CM2",
    "Q3_14day3", "Q3_14month3", "Q3_14year3", "Q3_14ageY3", "Q3_14ageM3",
    "Q3_14FT3", "Q3_14IN3", "Q3_14CM3",
    
    # Institutions 1-2
    "Q3_15inst1", "Q3_15town1", "Q3_15age1", "Q3_15from1", "Q3_15to1",
    "Q3_15inst2", "Q3_15town2", "Q3_15age2", "Q3_15from2", "Q3_15to2"
]

def get_original_name(section: str, schema_field: str, entry_num: int):
    """Reverse of rename_variable: Reconstructs the original variable name from a schema field and entry number.
    Used when regenerating original variable names for debugging or backward compatibility.
    - section: Determines which reverse mapping to use (e.g., 'pregnancies' vs 'physical_dev').
    - schema_field: The JSON field name (e.g., "EndDay").
    - entry_num: The pregnancy/measurement instance number.
    """
    config = section_registry.get(section)
    if not config:
        return None
    
    reverse_map = config['reverse_map']

    mapping = reverse_map.get(schema_field)
    if mapping is None:
        return None
    
    # Handle different mapping structures
    if isinstance(mapping, dict):
        # Pregnancy mappings use thresholds (1, 4, 7) to select the correct variable prefix
        for threshold in sorted(mapping.keys(), reverse=True):
            if entry_num >= threshold:
                return mapping[threshold](entry_num)
    elif callable(mapping):
        # Physical development mappings use direct lambda functions
        return mapping(entry_num)
    
    return None
