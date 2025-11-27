# -*- coding: utf-8 -*-
"""
Created on Thu Sep 11 14:50:18 2025

@author: shegde
purpose: Set of functions used to derive Morph Code for Breast and Ovarian data
"""

import os
os.chdir('N:\CancerEpidem\BrBreakthrough\SHegde\Schema_and_Derivation_utils\CancerSummary\scripts')
import sys
sys.path.append(os.path.abspath('N:\CancerEpidem\BrBreakthrough\SHegde\Schema_and_Derivation_utils'))


def derive_breast_morphology_code(data):
    """
    Derive ICD-10-O2 morphology code based on available fields

    Args:
        data (pandas dataframe): Input breast tumours dtafarme
    Returns:
        (string): ICD-10-O2 Morphology code
    
    """
    
    # If ICDMorphologyCode is already provided, use it
    if data.get('ICDMorphologyCode') and data['ICDMorphologyCode'] != 'null':
        if data.get('ICDMorphologyCode').startswith('M854') and data.get('PagetsDisease')=='Y':
            return data['ICDMorphologyCode']
        elif data.get('Microinvasion')=='P':
            return None
        elif data.get('ICDMorphologyCode').endswith('3') and data.get('InvasiveCarcinoma')=='P':
            return data['ICDMorphologyCode']
        elif data.get('ICDMorphologyCode').endswith('2') and data.get('InvasiveCarcinoma')=='N' and data.get('InsituCarcinoma')=='P':
            return data['ICDMorphologyCode']
        else:
            return None
        
    # Check malignancy status first
    if data.get('Malignant') != 'Y':
        return None  # Not malignant, no morphology code needed

    # Start with invasive carcinoma types
    if data.get('InvasiveCarcinoma') == 'P':
        invasive_type = data.get('Type')
        component = data.get('TypeComponent')
        
        if invasive_type == 'NST':
            if data.get('PagetsDisease') == 'Y':
                return '85413'
            else:
                return '85003'  # Infiltrating duct carcinoma, NOS
        
        elif invasive_type == 'PST':
            if component == 'Lob':
                return '85203'  # Lobular carcinoma
            elif component == 'Tub':
                return '82113'  # Tubular adenocarcinoma
            elif component == 'Crb':
                return '82013'  # Cribriform carcinoma
            elif component == 'Muc':
                return '84803'  # Mucinous adenocarcinoma
            elif component == 'Med':
                return '85103'  # Medullary carcinoma
            elif component == 'Pap':
                return '85033'  # Papillary carcinoma
            elif component == 'Mic':
                return '85233'  # Infiltrating duct mixed with other
            elif component == 'Tul':
                return '85243'  # Lobular tubular adenocarcinoma
            
        elif invasive_type =='MTT':
            return '85223'  # Infiltrating duct and lobular
            
        elif invasive_type =='MIX':
            return '85233'  # Infiltrating duct mixed with other
        
        else:
            return None
    
    elif data.get('InvasiveCarcinoma') == 'N':
        if data.get('InsituCarcinoma') == 'P':
            if data.get('DuctalCarcinomaInsitu') == 'P' and data.get('Cribiform_DCISGP') == 'Y':
                return '82012'
            elif data.get('DuctalCarcinomaInsitu') == 'P' and data.get('Solid_DCISGP') == 'Y':
                return '82302'
            elif data.get('DuctalCarcinomaInsitu') == 'P':
                return '85002'
            elif data.get('LobularCarcinomaInsitu') == 'Y':
                return '85202'
            elif data.get('LobularCarcinomaInsitu') == 'Y' and data.get('DuctalCarcinomaInsitu') == 'P':
                return '85222'
            else:
                return '85002'
    
    return None  # Unable to determine code


def derive_ovarian_morphology_code(data):
    """
    Derive ICD-10-O2 morphology code for ovarian tumors based on available fields

    Args:
        data (pandas dataframe): Input breast tumours dtafarme
    Returns:
        (string): ICD-10-O2 Morphology code
    """
    
    # Check primary site first - focus on ovarian-related sites
    primary_site = data.get('Primary_Site')
    ovarian_sites = ["Ovary", "Tubo-Ovarian", "Fallopian", "Ovarian, fallopian, peritoneal", "Gynae origin - NOS"]
    
    if primary_site and primary_site not in ovarian_sites:
        return None  # Not an ovarian primary
    
    # GERM CELL TUMORS (highest specificity)
    germ_cell_type = data.get('Primary_Germ_Cells')
    if germ_cell_type:
        germ_cell_codes = {
            "Dysgerminomas": "90603",
            "Choriocarcinoma": "91003", 
            "Embryonal Carcinomas": "90703",
            "Immature Teratomas": "90803",
            "Polyembryoma": "90723",
            "Mature Teratoma": "90801"  # Usually benign
        }
        if germ_cell_type in germ_cell_codes:
            return germ_cell_codes[germ_cell_type]
    
    # SEX CORD-STROMAL TUMORS
    sex_cord_type = data.get('Sex_Cord_Stromal_Cells')
    if sex_cord_type:
        sex_cord_codes = {
            "Granulosa-theca cell tumours": "86203",
            "Sertoli-Leydig cell tumours": "86313"
        }
        if sex_cord_type in sex_cord_codes:
            return sex_cord_codes[sex_cord_type]
    
    # BORDERLINE TUMORS (next priority)
    borderline_type = data.get('Borderline')
    if borderline_type and borderline_type != "Absent":
        borderline_codes = {
            "Serous": "84421",
            "Mucinous": "84721",
            "Low Grade Mucinous": "84721",
            "Endometrial": "83801",
            "Clear cell adenofibroma": "83131",
            "Clear Cell Adenofibroma": "83131",
            "Other": "84421"  # Default to serous borderline
        }
        if borderline_type in borderline_codes:
            return borderline_codes[borderline_type]
    
    # INVASIVE EPITHELIAL CARCINOMAS (main category)
    invasive_status = data.get('Invasive')
    epithelium_type = data.get('Epithelium')
    
    if invasive_status == "P" and epithelium_type:
        invasive_codes = {
            # High-grade serous
            "Serous High": "84613",
            "Serous - NOS": "84603",  # Default serous
            "Serous Low": "84603",
            
            # Clear cell
            "Clear Cell (automatically Grade 3)": "83103",
            
            # Endometrioid
            "Endometrioid": "83803",
            "Endometroid": "83803",  # Variant spelling
            "Endometroid, Mucinous, Clear Cell": "83803",  # Use endometrioid as primary
            
            # Mucinous
            "Mucinous": "84803",
            
            # Other types
            "Carcinosarcoma": "89803",
            "Undifferentiated (automatically Grade 3)": "80203",
            "Transitional": "81203",
            "Adenocarcinoma - NOS": "81403",
            "Pseudomyxoma peritoneii": "84806",  # Special code for pseudomyxoma
            
            # Mixed types
            "Mixed Epithelial Types": "83233",
            "Mixed Endometroid, Serous": "83233",
            "Mixed Endometrioid, Serous": "83233",
            "Endometrioid, Clear Cell": "83233",
            
            # Default
            "Other": "81403"  # Adenocarcinoma NOS
        }
        if epithelium_type in invasive_codes:
            return invasive_codes[epithelium_type]
    
    # BENIGN TUMORS
    benign_type = data.get('Benign_Tumour')
    if benign_type:
        benign_codes = {
            "Serous": "84410",
            "Mucinous": "84700", 
            "Other": "80000"  # Benign neoplasm NOS
        }
        if benign_type in benign_codes:
            return benign_codes[benign_type]