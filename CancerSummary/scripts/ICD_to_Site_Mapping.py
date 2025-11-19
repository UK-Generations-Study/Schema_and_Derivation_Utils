# -*- coding: utf-8 -*-
"""
Created on Tue Jul 1 10:20:27 2025

@author: shegde
purpose: Derive SITE variable using ICD CODE
"""
import pandas as pd

icd10_to_site_mapping = {
    
    # Lip, Oral Cavity and Pharynx (C00-C14)
    'C00': 'Lip', 'C01': 'Tongue', 'C02': 'Tongue', 'C03': 'Gum', 
    'C04': 'Mouth', 'C05': 'Palate', 'C06': 'Mouth',
    'C07': 'Parotid gland', 'C08': 'Salivary glands', 'C09': 'Tonsil',
    'C10': 'Oropharynx', 'C11': 'Nasopharynx', 'C12': 'Piriform sinus',
    'C13': 'Hypopharynx', 'C14': 'Other lip/oral cavity/pharynx',

    # Digestive Organs (C15-C26)
    'C15': 'Oesophagus', 'C16': 'Stomach', 'C17': 'Small intestine', 
    'C18': 'Colon', 'C19': 'Rectosigmoid junction', 'C20': 'Rectum',
    'C21': 'Anus and anal canal', 'C22': 'Liver and intrahepatic bile ducts',
    'C23': 'Gallbladder', 'C24': 'Other biliary tract', 'C25': 'Pancreas',
    'C26': 'Other digestive organs',

    # Respiratory and Intrathoracic Organs (C30-C39)
    'C30': 'Nasal cavity and middle ear', 'C31': 'Accessory sinuses',
    'C32': 'Larynx', 'C33': 'Trachea', 'C34': 'Bronchus and lung',
    'C37': 'Thymus', 'C38': 'Heart/mediastinum/pleura', 'C39': 'Other respiratory sites',

    # Bone and Articular Cartilage (C40-C41)
    'C40': 'Bone and Articular Cartilage', 'C41': 'Bone and Articular Cartilage',

    # Skin (C43-C44)
    'C43': 'Skin', 'C44': 'Skin',

    # Mesothelial and Soft Tissue (C45-C49)
    'C45': 'Mesothelioma', 'C46': "Kaposi sarcoma",
    'C47': 'Nervous system',
    'C48': 'Retroperitoneum and peritoneum', 'C49': 'Other connective and soft tissue',

    # Breast (C50)
    'C50': 'Breast',

    # Female Genital Organs (C51-C58)
    'C51': 'Vulva', 'C52': 'Vagina', 'C53': 'Uterus', 'C54': 'Uterus',
    'C55': 'Uterus', 'C56': 'Ovary', 'C57': 'Other female genital organs',
    'C58': 'Placenta',

    # Male Genital Organs (C60-C63)
    'C60': 'Penis', 'C61': 'Prostate', 'C62': 'Testis', 'C63': 'Other male genital organs',

    # Urinary Tract (C64-C68)
    'C64': 'Kidney', 'C65': 'Renal pelvis', 'C66': 'Ureter', 'C67': 'Bladder',
    'C68': 'Other urinary organs',

    # Eye, Brain and Central Nervous System (C69-C72)
    'C69': 'Eye and adnexa', 'C70': 'Meninges', 'C71': 'Brain',
    'C72': 'Central nervous system',

    # Thyroid and Other Endocrine Glands (C73-C75)
    'C73': 'Thyroid gland', 'C74': 'Adrenal gland', 'C75': 'Other endocrine glands',

    # Ill-defined, Secondary and Unspecified Sites (C76-C80)
    'C76': 'Other and ill-defined sites', 'C77': 'Lymph nodes',
    'C78': 'Secondary respiratory and digestive organs', 'C79': 'Secondary other sites',
    'C80': 'Unknown primary site',

    # Lymphoid, Hematopoietic and Related Tissue (C81-C96)
    'C81': 'Hodgkin lymphoma', 'C82': 'Follicular lymphoma', 'C83': 'Non-follicular lymphoma',
    'C84': 'Mature T/NK-cell lymphomas', 'C85': 'Unspecified non-Hodgkin lymphoma',
    'C86': 'Other specified T/NK-cell lymphomas', 'C88': 'Malignant immunoproliferative diseases',
    'C90': 'Plasma cell neoplasms',
    'C91': 'Lymphoid leukemia', 'C92': 'Myeloid leukemia', 'C93': 'Monocytic leukemia',
    'C94': 'Other leukemias of specified cell type', 'C95': 'Leukemia of unspecified cell type',
    'C96': 'Other and unspecified lymphoid, hematopoietic and related tissue',
    'C97': 'Independent multiple sites',

    # IN SITU NEOPLASMS (D00-D09)
    'D00': 'Oral cavity/oesophagus/stomach',
    'D01': 'Other digestive organs',
    'D02': 'Middle ear/respiratory system',
    'D03': 'Melanoma',
    'D04': 'Skin',
    'D05': 'Breast',
    'D06': 'Uterus',
    'D07': 'Other genital organs',
    'D09': 'Other and unspecified sites',

    # BENIGN NEOPLASMS (D10-D36)
    'D10': 'Mouth and pharynx',
    'D11': 'Major salivary glands',
    'D12': 'Colon, rectum, anus and anal canal',
    'D13': 'Ill-defined parts of digestive system',
    'D14': 'Middle ear and respiratory system',
    'D15': 'Other and unspecified intrathoracic organs',
    'D16': 'Bone and articular cartilage',
    'D17': 'Lipomatous neoplasm',
    'D18': 'Hemangioma and lymphangioma, any site',
    'D19': 'Mesothelial tissue',
    'D20': 'Soft tissue of retroperitoneum and peritoneum',
    'D21': 'Other connective and soft tissue',
    'D22': 'Melanocytic nevi',
    'D23': 'Skin',
    'D24': 'Breast',
    'D25': 'Uterus',
    'D26': 'Uterus',
    'D27': 'Ovary',
    'D28': 'Other and unspecified female genital organs',
    'D29': 'Male genital organs',
    'D30': 'Unary organs',
    'D31': 'Eye and adnexa',
    'D32': 'Meninges',
    'D33': 'Brain and other parts of central nervous system',
    'D34': 'Thyroid gland',
    'D35': 'Other and unspecified endocrine glands',
    'D36': 'Other and unspecified sites',

    # NEOPLASMS OF UNCERTAIN OR UNKNOWN BEHAVIOR (D37-D48)
    'D37': 'Oral cavity and digestive organs',
    'D38': 'Middle ear and respiratory and intrathoracic organs',
    'D39': 'Female genital organs',
    'D40': 'Male genital organs',
    'D41': 'Urinary organs',
    'D42': 'Meninges',
    'D43': 'Brain and central nervous system',
    'D44': 'Endocrine glands',
    'D45': 'Polycythemia vera',
    'D46': 'Myelodysplastic syndromes',
    'D47': 'Lymphoid, hematopoietic and related tissue',
    'D48': 'Other and unspecified sites',

}


def get_site_from_ICD(icd_code, s_study_id=None):
    """
    Comprehensive function that handles all neoplasm codes (C00-D49)

    Args:
        icd_code (string): ICD codefrom the source data
        s_study_id (string): Source of the ICD code

    Returns:
        dict: Mapping from ICD code to Cancer SITE
     """
     
    if pd.isna(icd_code) or str(icd_code).strip() in ['', 'nan', 'None']:
        # Use S_STUDY_ID fallback if ICD code is missing
        if s_study_id is not None and pd.notna(s_study_id):
            if 'HistoPath_BrCa' in s_study_id:
                return 'Breast'
            elif 'HistoPath_OvCa' in s_study_id:
                return 'Ovary'
        return 'Unknown'
    
    icd_code = str(icd_code).strip()
    code = icd_code[:3]
    
    if code in icd10_to_site_mapping:
        return icd10_to_site_mapping.get(code, 'Unknown')


# Mapping dictionary: keys are site names, values are lists of ICD code ranges
# https://pubmed.ncbi.nlm.nih.gov/33538338/
grouped_site_mapping = {
    'lip': ['C00-C06'],
    'oral cavity': ['C00-C06'],
    'salivary glands': ['C07-C08'],
    'oropharynx': ['C09-C10'],
    'nasopharynx': ['C11'],
    'hypopharynx': ['C12-C13'],
    'oesophagus': ['C15'],
    'stomach': ['C16'],
    'colorectal cancer': ['C18-C21'],  # combined colon, rectum, anus
    'liver': ['C22'],
    'gallbladder': ['C23'],
    'pancreas': ['C25'],
    'larynx': ['C32'],
    'lung': ['C33-C34'],
    'melanoma of skin': ['C43'],
    'NMSC': ['C44'],
    'mesothelioma': ['C45'],
    'Kaposi sarcoma': ['C46'],
    'female breast': ['C50'],
    'vulva': ['C51'],
    'vagina': ['C52'],
    'cervix uteri': ['C53'],
    'corpus uteri': ['C54'],
    'ovary': ['C56'],
    'penis': ['C60'],
    'prostate': ['C61'],
    'testis': ['C62'],
    'kidney': ['C64-C65'],
    'bladder': ['C67'],
    'brain, CNS': ['C70-C72'],
    'thyroid': ['C73'],
    'Hodgkin lymphoma': ['C81'],
    'non-Hodgkin lymphoma': ['C82-C86', 'C96'],
    'multiple myeloma': ['C88', 'C90'],
    'leukemia': ['C91-C95']
}

# Helper function to check if ICD code falls in range
def group_sites(icd):
    if pd.isna(icd):
        return 'Unknown'

    icd3 = icd[:3]
    for site, ranges in grouped_site_mapping.items():
        for r in ranges:
            if '-' in r:
                start, end = r.split('-')
                if start <= icd3 <= end:
                    return site
            else:
                if icd3 == r:
                    return site
    return 'Unknown'
