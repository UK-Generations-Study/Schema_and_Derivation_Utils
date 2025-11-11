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

`json_schemas/`

Schemas that define the expected JSON structure for each questionnaire and document the provenance of the variable.

- `raw/`
   
   Original "as-collected" schemas for each section (before pseudo-anonymisation and date aggregation/derivation).

- `post_pii/`
   
   Post-processing schemas that describe the final pseu-doanonymised output of R0 non-derived variables:

    - `StudyID` -> `TCode`.
    - Raw date componenets aggregated to pseudo-anonymised complete dates.
    - PII fields dropped.   

# Pipeline Flow
ETL step-by-step:
1. Extract from source (SQL).
2. Pivot/standardise variables.
3. Clean and type-cast according to schemas.
4. Restructure into nested JSON.
5. Pseudo-anonymise.
6. Validate against new PII JSON schemas.
7. Output and QC reports.

# Installation & Prerequisites
Python version 3.11.5

Python packages installed:

- pandas
- numpy
- jsonschema
- SQLAlchemy
- pyodbc

Microsoft Access Driver for SQL.

# Configuration
The paths for schemas, output data, QC, etc. are in `config_utils.py` which can all be updated by adjusting the base `delivery_process` string variable. You must also make sure you have a SQL account that you can connect through a Microsoft Access Driver. 

# Running the ETL
Run the `run_all_sections.py` script, and that will run the full ETL. For example, in VS Code, press the play button when opening the Python script. Or, using a Command Line Interface (CLI), `python run_all_sections.py`.

Typical run time for the whole ETL for all sections is around 3 hours.

# Schemas & Validation
The ETL is schema-driven. Every transformation step is designed to produce JSON that conforms to an explicit JSON Schema.

**1. Schema layout**

    - Raw input schemas
   
    Describe the structure of the section as it comes from the questionnaire.
       
        - `schemas/raw/<SectionName>_JSON.json`

    - Pseudo-anonymised output schemas

      Describe the final JSON written out after:
     
        - Replacing `StudyID` with `TCode`.

        - Aggregating and deriving combined date fields.

        - Removing pii and date componenents.

        - `schemas/pseudo_anon/<SectionName>_PseudoAnon.json`

    -Each pseudo-anon schema keeps a reference back to the raw schema (via $defs), so you can trace where fields came from.

**2. How schemas are used in the ETL**

    a. Load schema
   
    For each section, the script loads the raw schema, and `load_schema` also resolves any $ref references so the rest of the pipeline sees a fully expanded schema.
      
    b. Drive cleaning and types

    The schema provides the following JSON built-in schema keywords that are extracted and used in cleaning processing (`process_nested_data`):

        - expected JSON types to cast values to the correct type (`string`, `integer`, `number`, etc.)
      
        - numeric bounds where values outside are set to null (`minimum`, `maximum`)
  
            Only use maximum if it is a finite numeric scale or categorical numeric value, i.e. not continuous (day of the month, numerical value of month in the year, etc.)
  
            Only use minimum where values cannot be negative (height, weight, age, etc.) 
      
        - allowed values that guide recodes of special values and unknown codes (enum)

        The schema provides the following custom annotations that are not built in to JSON validation, but help the user with processing or context:

        - question ID that corresponds to the SQL metadata where questions that ask the same contextual question have the same ID (`questionID`)
     
        - variable name as it corresponds to the SQL database, differs depending on level of variable (`name`):
     
        Flat (non-array) fields use the original raw variable names from SQL. These are 1:1 with the human-readable variable names that were created when the data were first collected.

        For array fields, we use new, human-readable schema field names instead. Arrays are repeated structures and the underlying raw variable names follow inconsistent patterns, so a single raw name cannot reliably represent all repeats. Using stable schema field names keeps the schemas compact and easier to understand.

        - question as it was written in the questionnaire (`question`)
     
        - description of the variable and question to help the user decipher the context and use of the variable (`description`)
     
        - allowed value descriptions defining what numeric values mean (`enumDescriptions`)
     
        - answer IDs where repeated answers have the same ID that correlate to the metadata (`answerID`)

    c. Drive restructuring

    The schema also defines where fields live in the nested structure (arrays, objects, index fields). `restructure_utils` uses this metadata, together with `nested_utils`, to build the final nested JSON objects that match the schema structure.
      
    d. Pseudo-anonymised schema generation
  
    After restructuring, pseudo_anon_utils takes the raw schema and:

        - inserts new derived date fields
      
        - removes raw date components and PII fields
      
        - replaces R0_StudyID with R0_TCode
      
    The result is the pseudo-anonymised schema, which is what we validate the final output against.

3. Validation process

    Validation is performed using the JSON schema through a helper in `common_utils`.

    Any validation errors are:

        - logged with the record index and field path
   
        - summarised for review in the ETL logs or in the CLI

    If errors is empty, the section’s JSON is considered structurally valid.

5. Adding or updating schemas

    When extending or updating the ETL:

        1. Add/update the raw schema under `schemas/raw/`.

        2. Regenerate the pseudo-anon schema via `pseudo-anon utilities`.

        3. Run the ETL + validation for that section and review any schema errors.

# Logging & QC


# Error Handling & Troubleshooting


# Extending the ETL


# Data Privacy & Security


# Testing


# License & Ownership


# Contacts
