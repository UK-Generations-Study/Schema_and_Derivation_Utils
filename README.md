# Generations Study Questionnaire Data ETL
Repo for maintaining JSON schemas, scripts, and non-PII quality check (QC) outputs to produce derived datasets for Generations Study (GS) data.
This README was last updated 10/11/2025.

# Overview & Background
The Generations Study questionnaire data began collection in 2004 on paper using Optical Character Recognition (OCR) to read each questionnaire into database storable data. This methodology for data collection continued throughout baseline data collection acros the cohort. This ETL process aims to better-document the current state of the data, update and siplify the data processing, and make the derivation methodology of variables from the raw data available to the public.

# Data Scope
As of the last update this ETL applies to all Baseline (R0) questionnaire data. There are 19 raw sections in the ETL, plus an additional raw derivation section for variables that need to be derived within the secure server to preserve participant identities. The raw data read in covers over 1,850 different questions from the SQL database they are stored in, while the output processed data covers over 950 variables in JSON format due to aggregation of date fields and teh removal of variables that potentially contain Personally Identifying Information (PII). Following is the list of all raw sections of baseline data:
- Alcohol, Smoking & Diet
- Birth Details
- Breast Cancer
- Breast Disease
- Cancer in Relatives
- Contraceptive & Hormone Replacement Therapy
- General Information
- Jobs
- Mammograms
- Medical History Cancers & Benign Tumors
- Medical History Drugs & Supplements
- Medical History Illnesses
- Menstrual & Menopause
- Other Breast Surgery
- Other Lifestyle Factors
- Physical Activity
- Physical Development
- Pregnancies
- X-Rays

# Repository Structure
Questionnaire/
   └───R0/
       ├───json_schemas/
       │   ├───post_pii/
       │   └───raw/     
       ├───scripts/   
       └───validation/
           └───{section}_ValidationSummary/

`json_schemas`

# Pipeline Flow


# Installation & Prerequisites


# Configuration


# Running the ETL


# Schemas & Validation


# Logging & QC


# Error Handling & Troubleshooting


# Extending the ETL


# Data Privacy & Security


# Testing


# License & Ownership


# Contacts
