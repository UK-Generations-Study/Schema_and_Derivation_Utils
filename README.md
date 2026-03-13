# 1. Generations Study Questionnaire Data ETL

Repo for maintaining JSON schemas, scripts, and non-PII quality check (QC) outputs to produce derived datasets for Generations Study (GS) data.

This README was last updated 11/03/2026.

# 2. Overview & Background

The Generations Study questionnaire data began collection in 2004 on paper using Optical Character Recognition (OCR) to read each questionnaire into database storable data. This methodology for data collection continued throughout baseline data collection across the cohort. This ETL process aims to better-document the current state of the data, update and simplify the data processing, and make the derivation methodology of variables from the raw data available to the public.

This ETL creates data and metadata in JSON and JSON schema, respectively. It is advised that, if the user is new to JSON formats, before attempting to work with the data, the user reads through and completes the exercises in the following JSON reference lesson: [Introduction to JSON](https://json-schema.org/docs).

# 3. Data Scope

As of the last update this ETL applies to all Baseline (R0) questionnaire data. There are 19 raw and pseudo-anonymised sections in the ETL, plus an additional core derived variables section for variables that are often used by analysts. All data that is maintained outside of the Trusted Research Environment (TRE) has been pseudon-anonymised to preserve identities of the participants. The raw data read in covers over 1,850 different questions from the SQL database they are stored in, while the output processed pseudo-anonymised data covers over 950 variables in JSON format due to aggregation of date fields and the removal of variables that potentially contain Personally Identifying Information (PII). Following is the list of all raw sections of baseline data, plus the core derived data set below:
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

- Core Derived Variables

The core variable derivatios use the outputs from the pseudo-anonymisation process, and information from the AdminEvents data. AdminEvents uses the SQL Mailing database (with variables `ADOB`, `EventDate`, `Random` and `TCode`) to create a small set of event variables that are safe to export: shifted date of birth (`DOB_Shifted`), unshifted year of birth (`YOB`), shifted date of entry (`EntryDate_Shifted`), unshifted year of entry (`EntryYear`), and age in whole years at entry (`AgeEntry`), along with other helpful administrative variables. Details on Admin Events can be found here: LINK TO README ON ADMIN EVENTS.

# 4. Repository Structure
        Questionnaire/
            └───R0/
                ├───schemas/
                │   ├───pseudo_anon/
                │   └───raw/
                ├───scripts/
                └───validation/
                    └───<SectionName>_ValidationSummary/

- `schemas/`

  Schemas that define the expected JSON structure for each questionnaire section and document the provenance of the variable.

  - `raw/`
           
    Original "as-collected" schemas for each section (before pseudo-anonymisation and date aggregation/derivation).
        
  - `post_pii/`
           
    Post-processing schemas that describe the final pseudo-anonymised output of R0 non-derived variables to be used in analyses and to derive core variables:

- `scripts/`
  
  Code to run the ETL and derivation steps. Raw data processing, and pseudo-anonymised processing are executed in the TRE, and variable derivation occurs outside the TRE:

  - Questionnaire ETL consists of `run_all_sections.py` plus helper modules (`cleaning_utils.py`, `nested_utils.py`, `pseudo_anon_utils.py`, `schema_utils.py`, `qc_utils.py`, etc.) to:

    - extract from SQL
    - pivot/clean
    - restructure to nested JSON
    - pseudo-anonymise and validate against the pseudo-anonymised schemas.

  - Derivation – Derivation.ipynb uses the outputs from the above ETL and consists of a series of functions that derive a single variable or group of similarly derived variables:
  
    - extract from pseudo-anonymised data and AdminEvents
    - process and manipulate data
    - write to flat json
 
      These are written to a JSON keyed and linked by TCode and then used alongside the other data outputs, like Pathology, Cancer Summaries, Outcomes, etc.

- `validation/`

  Per-section QC outputs:

  - `<SectionName>_ValidationSummary/` folders hold:

    - resolver index (raw -> schema field mapping)
    - JSON Schema validation output
    - QC JSONs such as `variable_check.json` and `value_reconciliation.json`.

# 5. Pipeline Flow
Raw processing ETL step-by-step:
1. Extract from source (SQL).
2. Pivot/standardise variables.
3. Clean and type-cast according to schemas.
4. Restructure into nested JSON.
5. Pseudo-anonymise.
6. Validate against new PII JSON schemas.
7. Output and QC reports.

Runing `Derivation.ipynb` occurs separately as its own task and is self contained in one script. THis also occurs after the pseudo-anonymised data are brought out of the TRE.

# 6. Installation & Prerequisites

Before running the ETL, ensure you have:

**Python**
  
  - Python 3.10+.
  - Recommended: Python 3.11 or later.
  - Dependencies are listed in `requirements.txt`, and are:

        jsonschema==4.26.0
        matplotlib==3.10.8
        numpy==2.4.3
        pandas==3.0.1
        pyodbc==5.3.0
        scipy==1.17.1
        SQLAlchemy==2.0.48
 
**Operating system and environment**

  - A machine with access to the Windows network paths used in `config.py`.
  - Ability to create and activate a virtual environment (e.g. `venv`, `conda`, `uv`).

**Database & drivers**

  - Appropriate ODBC drivers installed, matching the `config.py` settings:
    
    - `ODBC Driver 17 for SQL Server` (32-bit)
    - `SQL Server` (64-bit)
    - Microsoft Access Driver for SQL.

# 7. Configuration
The paths for schemas, output data, QC, etc. are in `config_utils.py` which can all be updated. You must also make sure you have a SQL account that you can connect through a Microsoft Access Driver. 

# 8. Running the ETL
Run the `run_all_sections.py` script, and that will run the full ETL. For example, in VS Code, press the play button when opening the Python script. Or, using a Command Line Interface (CLI), `python run_all_sections.py`.

On current infrastructure, a full run for all sections takes on the order of a few hours. Runtime will vary by environment.

# 9. Schemas & Validation
The ETL is schema-driven. Every transformation step is designed to produce JSON that conforms to an explicit JSON Schema. If the data do not conform, the ETL validation step reports the first validation errors encountered for review. As of the date this document was last edited, there were no errors during validation for any files.

## 9.1. Schema layout
- Raw input schemas describe the structure of each questionnaire section (schemas/raw/<SectionName>_Schema.json) and define the expected fields, types, constraints, and metadata used during ETL processing.
    
- Pseudo-anonymised output schemas describe the final JSON written out (schemas/pseudo_anon/<SectionName>_Schema_PseudoAnon.json), after:

    - Replacing `StudyID` with `TCode`.
    - Aggregating and deriving combined date fields.
    - Shifting date fields by a random number stored in SQL per participant.
    - Removing PII variables and date components.

- Each pseudo-anon schema is generated from the raw schema and updated to reflect derived fields, removed PII fields, and pseudo-anonymised identifiers.

## 9.2. How schemas are used in the ETL

This section will be most helpful for data managers and developers.

**9.2.a. Load schema**

For each section, the script loads the raw schema using load_schema. If the schema contains $ref references, these are resolved so that downstream processing operates on a fully expanded schema structure.
  
**9.2.b. Schema driven cleaning and types**

The ETL uses the schema to drive the processing of data in the following ways:

_**9.2.b.1 Built-in keywords**_

The schema provides the following JSON built-in schema keywords that are extracted and used in cleaning processing (`process_nested_data` and related cleaning utilities):

- expected JSON types to cast values to the correct type (string, integer, number, etc.), extracted from the schema and used to guide type conversion during cleaning.
- numeric bounds where values outside the valid range are set to null (minimum, maximum)

    Only use maximum if it is a finite numeric scale or categorical numeric value, i.e. not continuous (day of the month, numerical value of month in the year, etc.)

    Only use minimum where values cannot be negative (height, weight, age, etc.) 

- allowed values that guide validation and cleaning of categorical values (enum)

_**9.2.b.2 Custom annotations**_

The schema provides the following custom annotations that are not built in to JSON validation, but help the user with processing or context. All custom annotations are represented by starting with "x-":

`x-questionID`
- The numeric ID of the questionnaire question in the SQL metadata.
- Questions that refer to the same conceptual question share the same ID even if they appear in different contexts (for example inside repeated structures).
- This allows the ETL and downstream metadata lookups to align schema variables with questionnaire metadata stored in SQL.#

`x-name`
- The variable name corresponding to the SQL database `VarName` column.
- This is used by the ETL variable resolver to map raw SQL variable names to schema fields.
- The meaning of `x-name` differs depending on whether the variable is flat or part of a repeated array structure.

**_Flat (non-array) fields_**

These use the original SQL variable name.

"x-name": "Q7_1_1_1"

There is only one value per participant and the variable always refers to the same question, so the schema can directly use the SQL variable name.

 **_Array fields (repeated structures)_**
 
These represent repeated questionnaire structures such as:

- pregnancies
- drug treatments
- jobs
- mammogram events

The SQL database often stores these using inconsistent naming patterns. For example:

                `Q6_1_1_1`
                `Q6_1_2_1`
                `Q6_1_3_1`
                `ocname4o`

These all represent the same logical variable across different instances.

Instead of encoding the instance number in the schema variable name, the schema uses a stable variable name:

                `ContracepPill_Name`

The ETL then uses the resolver logic to detect the instance number from the raw variable name and place the value in the correct array index.

This keeps schemas:

- more compact
- easier to read
- stable even when raw SQL naming changes.

`x-question`

- The exact wording of the question as it appeared in the questionnaire.
- This helps users interpret variables without needing to consult the questionnaire documentation.

`x-enumDescriptions`

- Human-readable explanations for enumerated numeric values.
- These descriptions provide context for categorical variables whose values are stored as numeric codes.

`x-answerIDs`

- Answer identifiers that correspond to the questionnaire metadata tables.
- These IDs match the AnswerID values stored in the questionnaire metadata and allow direct linkage between schema variables and the database representation of questionnaire responses.

`x-derivedFrom`

- Used in pseudo-anonymised schemas to document which raw variables were used to derive a field.
- This annotation provides traceability between derived variables and their original questionnaire fields.
- It is primarily used for:

          pseudo-anonymised date fields
          derived variables created during the ETL.

**_Schema-level annotations_**

In addition to field-level annotations, schemas contain metadata describing the dataset itself.

`x-version`

- Where the version of the data that the schema is related to is populated. For example, `1.0.0`.

`x-provenance`

- This block records data round, repository location, schema maintainer, and last modified date.
- This metadata provides versioning and traceability for schema definitions.

**9.2.c. Drive restructuring**

The schema also defines where fields live in the nested structure (arrays, objects, index fields). `restructure_utils` uses this metadata, together with `nested_utils`, to build the final nested JSON objects that match the schema structure.
  
**9.2.d. Pseudo-anonymised schema generation**

After restructuring, `pseudo_anon_utils` derives dates, replaces identifiers, and updates the schema to describe the pseudo-anonymised output.

- inserts new derived date fields
- removes raw date components and PII fields
- replaces `StudyID` with `RTCode`
  
The result is the pseudo-anonymised schema, which is what we validate the final output against.

## 9.3. Validation process

Validation is performed using the JSON Schema validator through helper utilities in `common_utils`.

Any validation errors are:

- logged with the record index and field path
- summarised for review in ETL logs and validation outputs

If no errors, the section's JSON is considered structurally valid.

## 9.4. Adding or updating schemas

When extending or updating the ETL:

1. Add/update the raw schema under `schemas/raw/`.
2. Regenerate the pseudo-anon schema via the pseudo-anonymisation utilities (`update_schema`).
3. Run the ETL + validation for that section and review any schema errors.

# 10. Logging & QC

This ETL is designed to surface problems early via structured logs and a set of QC artefacts generated per section.

## 10.1. Runtime logging

- The ETL uses a standard Python `logging` logger configured in the entry scripts / notebooks.
- Typical log messages include:
- Start/end of each major stage (extract, pivot, clean, restructure, pseudo-anon, validate).
- Row counts before/after key operations.
- Summaries of validation and QC checks.

## 10.2. Change-tracking output

During cleaning, the ETL records any value-level changes (e.g. `"1"` -> `1`, out-of-range -> `null`) into a change-tracking structure. Due to sensitivity of some of the data in the files, the change-tracking JSONs have not been uploaded to the repo.

- Saved to: `validation/<CHANGE_TRACKING_DIR>/<SECTION_SLUG>_change_tracking.json`
- Structure (per section):

        {
          "R0_000001": {
            "R0_FieldName": [
            {"old": "1", "new": 1},
            {"old": -10: "new": null}
            ]
          }
        }

Change-tracking is used by `qc_utils` to explain differences in value distributions before and after ETL.

## 10.3. Validation and resolver summaries

For each section, schema validation and variable resolution generate artefacts under a `ValidationSummary` directory at the end of each section's ETL.

- Root directory (per round): `validation/`
- Per-section subfolder:

    - `validation/<SectionName>_ValidationSummary/`

Inside each section's folder you will find:

- `<SectionSlug>_resolver_index.json`
- `Built by restructure_utils.build_resolver_cache_from_columns`
- Maps schema fields to the raw variables that populate them (and their index bands).

## 10.4. QC reports

Outputs include:

- Value frequency reconciliation:

    - Compares value counts in the raw pivot vs. the final JSON.
    - Uses change-tracking to explain where values changed.
    - Saved to: `validation/<SectionName>_ValidationSummary/value_reconciliation.json`
 
- Variable alignment check:

    - Maps SQL variable names to new, human readable variable names.
    - Utilizes the resolver.
    - Checks against removed date and PII fields to make sure all variables are accounted for.
    - Saved to: `validation/<SectionName>_ValidationSummary/variable_check.json`

These artefacts help answer "which raw variable ended up in which JSON field?", "why did validation fail?", "are all variables accounted for?".

## 10.5. Interpreting failures

If a section fails QC or validation:

- Check the log file in for stack traces and high-level error messages.
- Inspect the JSON validation output to see which fields broke schema rules.
- Open the resolver cache (`<SECTION_SLUG>_resolver_index.json`) to verify that variables are mapped to the expected schema fields.
- Review change-tracking and QC outputs for unexpected occurrences:
- Look for unexpected large numbers of changes for a single field.

If necessary, re-run the section notebook with a higher log level (e.g. `DEBUG=True`) and a smaller sample size to iterate quickly on issues.

# 11. Data Privacy & Security

This ETL is designed to work with sensitive questionnaire data, so data protection is built into both the code and the workflow.

## 11.1 Pseudo-anonymisation

The pipeline never writes out raw identifiers directly from the source database and the data follows a similar pseudo-anonymisation process as the schema in 9.2.d:

- `StudyID` replaced pseudo-identifier stored in private server, `TCode`.
- Date components aggregated to derived dates.

    - Inside the TRE, `AdminEvents.ipynb` uses actual dates of birth and entry (`ADOB`, `EventDate`) together with the participant-specific `Random` offset to create:
    
      - Shifted dates (`DOB`, `EntryDate`) that mask the true calendar dates but preserve within-person intervals.
      - Unshifted year-only and age variables (`YOB`, `EntryYear`, `AgeEntry`) that are safe to export.

    - In the questionnaire ETL, individual day/month/year components from the raw R0 tables are aggregated to derived dates and then shifted using the same Random offset logic, with partial dates completed according to pre-defined rules as follows: aggregated dates with no day of the month are given the 15th day of the month, aggregated dates with no month values are given July, and if both day and month are missing the values given are the 1st of July. Then, the format that the date came in is preserved and used in the no-PII data, i.e. if the day was originally missing from the questionairre and became the 15th for shifting, the day will not show in the no-PII data to protect the exact number of days that the participant has their dates shifted by. Only these derived, shifted or aggregated forms appear in the processed JSON; the original date components and Random remain on the secure server.
 
- Removal of direct PII data.

    - No variables that have identifying information, or potentially identifying information are included in the processed data
    - Includes names, addresses, names of towns, names of hospitals, and any variable that was asked as open ended text like medications, cancer types, and familial relationships.
 
## 11.2 Handling of real data

- This repo is intended to contain code and schemas only. 
- Real questionnaire data, SID mappings, and any intermediate extracts must not be committed to Git or shared via this repository.
- Database connection details (servers, usernames, passwords) are removed in `config` and must not be committed to the repo.
 
## 11.3 Access control and usage

- This ETL is intended for use by authorised team members only, in compliance with:

    - The study's data governance policies, and any applicable legal/regulatory requirements (e.g. GDPR for EU/UK datasets).

- Users running the ETL are responsible for:

    - Ensuring they have permission to access the underlying databases.
    - Not exporting or sharing outputs outside approved channels.

# 12. License / Usage

Internal ICR use only. Do not distribute or reuse without appropriate approvals.

# 13. Contributing

If you need to extend or modify the ETL, raise an issue or discuss changes with the data engineering / GS team.

# 14. Contacts

Please contact Tal Cohen, tal.cohen@icr.ac.uk, if you have any questions or concerns.
