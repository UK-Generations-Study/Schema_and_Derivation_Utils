# UK Generations Study — JSON Schema Conventions and Rules

> **Scope:**  All JSON schemas developed for UK Generations study 
> **JSON Schema version:** 2020-12 - https://json-schema.org/draft/2020-12#draft-2020-12
> **Last updated:**  2026-03-05

## Contents

1. [File Naming and Purpose](#1-file-naming-and-purpose)
2. [Root Structure and Mandatory Keys](#2-root-structure-and-mandatory-keys)
3. [The Identifier Field TCode / StudyID](#3-the-identifier-field-tcode--studyid)
4. [Type Declarations and Nullability](#4-type-declarations-and-nullability)
5. [Enums and Descriptions](#5-enums-and-descriptions)
6. [Numeric Constraints and Units](#6-numeric-constraints-and-units)
7. [Dates and Privacy](#7-dates-and-privacy)
8. [Advanced Patterns oneOf and Arrays](#8-advanced-patterns-oneof-and-arrays)
9. [Custom Extensions x-*](#9-custom-extensions-x-)
10. [Questionnaire-specific Fields](#10-questionnaire-specific-fields)
11. [additionalProperties](#11-additionalproperties)
12. [Versioning and Provenance](#12-versioning-and-provenance)
13. [Canonical $id URLs](#13-canonical-id-urls)
14. [Common Mistakes](#14-common-mistakes)
15. [Validation](#15-validation)

---

## 1. File Naming and Purpose

Use **PascalCase** for topic names. No spaces or special characters.

| Area | Pattern | Example |
|---|---|---|
| Questionnaire raw | `{Topic}_Schema_PseudoAnon.json` | `GeneralInformation_Schema_PseudoAnon.json` |
| Questionnaire derived | `DerivedVariables_Schema.json` | *(Single file)* |
| Clinical / outcomes | `{Topic}_Schema.json` | `Outcomes_Schema.json` |
| Linked clinical data | `{Topic}_Schema_PseudoAnon.json` | `BreastTumourLink_Schema_PseudoAnon.json` |

> **Important:** Always include `_PseudoAnon` for any file containing pseudonymised participant-level data.

---

## 2. Root Structure and Mandatory Keys

Every schema must contain these keys in **exact order**. Do not add non-standard root keys (like `roundID`); use `x-provenance` instead.

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://github.com/UK-Generations-Study/Schema_and_Derivation_Utils/tree/main/path_to_schema.json",
  "title": "Human-readable name e.g. 'General information from R0 questionnaire'",
  "description": "Single line in plain English. No codes, no derivation logic.",
  "type": "object",
  "required": ["TCode"],
  "additionalProperties": false,
  "x-version": "1.0.0",
  "x-provenance": {
    "x-dataRound": "R0",
    "x-lastModified": "YYYY-MM-DD",
    "x-repository": "https://github.com/UK-Generations-Study/Schema_and_Derivation_Utils",
    "x-maintainer": "UK Generations Study"
  },
  "properties": { ... }
}
```
### `$defs`

-   Optional — only add if a **structure repeats within the same schema**
    
-   Not shared across schemas
    
-   Used for **local reusable objects**
```json
"$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://www.breakthroughgenerations.org.uk/schemas/{Filename}.json",
  "title": "Human-readable name",
  "description": "Plain English summary.",
  "type": "object",
  "required": ["TCode"],
  "additionalProperties": false,
  "x-version": "1.0.0",
  "x-provenance": {
    "x-dataRound": "R0",
    "x-lastModified": "YYYY-MM-DD",
    "x-repository": "https://github.com/UK-Generations-Study/Schema_and_Derivation_Utils",
    "x-maintainer": "UK Generations Study"
  },
  "properties": { ... },
  "$defs": {
	  "address": {
	    "type": "object",
	    "additionalProperties": false,
	    "required": ["street", "city", "country"],
	    "properties": {
	      "street": {
	        "type": "string",
	        "minLength": 1,
	        "maxLength": 255
	      },
	      "city": {
	        "type": "string",
	        "minLength": 1,
	        "maxLength": 100
	      },
	      "state": {
	        "type": "string",
	        "minLength": 2,
	        "maxLength": 100
	      },
	      "postalCode": {
	        "type": "string",
	        "pattern": "^[A-Za-z0-9\\- ]{3,12}$"
	      },
	      "country": {
	        "type": "string",
	        "pattern": "^[A-Z]{2}$",
	        "description": "ISO 3166-1 alpha-2 country code"
      }
    }
  }
}
```
---

## 3. The Identifier Field (TCode / StudyID)

Every schema must include a **participant identifier** in:

- `properties`  
- `required` array

```json
"TCode": {
  "description": "Pseudo-anonymized 8-character study identifier.",
  "type": "string",
  "minLength": 8,
  "maxLength": 8
}
```  
```json  
"StudyID": {  
	"description": "Unique identifier for a person in Generations data",  
	"type": "string",  
	"minLength": 6,  
	"maxLength": 6,  
	"x-description": "This variable is created internally by the Generations study team and is a PII variable"  
}  
```  
  
### Rules  
  
- Both `TCode` and `StudyID` must be **scalar strings** (never nullable)  
- `TCode` length: 8 characters  
- `StudyID` length: 6 characters  
- Both must be included in the `required` array, with `TCode` listed first

## 4. Type Declarations and Nullability

### Scalar Form

```json
"type": "string"
```

**Incorrect:**

```json
"type": ["string"]
```

---

### Nullable Fields

Most fields are nullable:

```json
"type": ["integer", "null"]
```

---

### Boolean Flags

Used only for **internal/admin flags**:

```json
"type": "boolean"
```

Nullable:

```json
"type": ["boolean", "null"]
```

---

### Type Selection Guide

| Data Category | JSON Type | Notes |
|---|---|---|
| Free text | `"string"` | Non-nullable if mandatory |
| Coded (Numbers) | `["integer", "null"]` | Integers representing categorical codes |
| Coded (Letters) | `["string", "null"]` | Characters representing categorical codes |
| Continuous | `["number", "null"]` | Measurements (BMI, etc.) |
| Date-only | `["string", "null"]` | `"format": "date"` (YYYY-MM-DD) |
| Datetime | `["string", "null"]` | `"format": "date-time"` (YYYY-MM-DDThh:mm:ssZ) |
| Year (4-digit) | `["integer", "null"]` | Do **not** use date format |

---

## 5. Enums and Descriptions

### Rules

- `enum`: list allowed values; `null` **always last**  
- `x-enumDescriptions`: human-readable, one entry per code  

**Correct example:**

```json
"enum": [1,2,null],
"x-enumDescriptions": [
  "1: Yes.",
  "2: No.",
  "null: Missing or invalid."
]
```

**Incorrect:** `"1,2: Yes or No"` (grouped entries forbidden)

---

### Enum Description Rules

| Rule | Correct | Incorrect |
|---|---|---|
| One entry per value | `"1: Yes."` | `"1, 2: Yes or No."` |
| Code must match enum | `"1: ..."` (enum contains `1`) | `"1: ..."` (enum lacks `1`) |
| No trailing spaces | `"1: Yes."` | `"1: Yes. "` |
| No blank descriptions | `"G: Grade recorded without further specification"` | `"G: "` |
| Null entry last | `"null: Missing or invalid."` | *(absent)* |

---

### Standard Null-like Descriptions

| Code | Description |
|---|---|
| `"null"` | `"null: Missing or invalid."` |
| `"U"` | `"U: Unknown"` |
| `"X"` | `"X: Not performed"` |
| `"9999"` | `"9999: Not applicable"` (adjust to match code) |

---

## 6. Numeric Constraints and Units

- Minimums: `"minimum": 0` for counts, sizes, durations, ages unless negative allowed  
- Age: `"minimum": 0`, `"maximum": 120` maximum to be put only if known 
- Year of Birth: `"minimum": 1900` maximum to be put only if known
- Units: always include `"x-unit": "years" | "cm" | "kg/m²" | "%"`  

---

## 7. Dates and Privacy

| Field Type | JSON Type | Format | Example |
|---|---|---|---|
| Date | `["string", "null"]` | `"date"` | `YYYY-MM-DD` |
| Datetime | `["string", "null"]` | `"date-time"` | `YYYY-MM-DDThh:mm:ssZ` |

**Rules:**

- Never store dates as integers (`20240101`) or free text  
- `_Shifted` fields must include:
  - `x-description` (privacy offset note)
  - `x-derivedFrom` referencing original date + offset  
- Years only: `"type": ["integer", "null"]`  
- Do not store shifted dates unless field name indicates it (`DOB_Shifted`)  

---

## 8. Advanced Patterns (oneOf and Arrays)

- **oneOf + sentinel values:** distinguish `null` from `9999` (not applicable)  
- **Arrays:** repeated records (minItems, maxItems), description should state `(one record per {thing})`  
- Inner objects: `"additionalProperties": false` + own required fields  

---

## 9. Custom Extensions (x-*)

| Keyword | Usage |
|---|---|
| `x-version` | Semantic version (MAJOR.MINOR.PATCH) |
| `x-description` | Technical/derivation details |
| `x-derivedFrom` | Array of source references |
| `x-formerName` | Previous variable name |
| `x-calculation` | Formula referencing `x-derivedFrom` fields |
| `x-provenance` | Root metadata block |

---

## 10. Questionnaire-specific Fields

### Raw questionnaire schemas (`*_Schema_PseudoAnon.json`)

```json
"R0_SomeQuestion": {
  "x-questionID": 42,
  "x-question": "Full text of the question as it appeared in the questionnaire.",
  "description": "Plain-English summary of what this field captures.",
  "type": ["integer", "null"],
  "enum": [0, 1, 2, null],
  "x-enumDescriptions": [
    "0: ...",
    "1: ...",
    "2: ...",
    "null: Missing or invalid."
  ],
  "x-answerID": [10, 1, 2]
}
```

| Field | Required? | Purpose |
|---|---|---|
| `x-questionID` | Yes | Links to question master table |
| `x-question` | Yes | Verbatim question text |
| `x-answerID` | Yes (if answers exist) | Links to answer master table |
| `description` | Always | Human-readable summary |

### Derived variables
The `x-derivedFrom` field records the **source fields used to derive a variable**.  
It must always be an **array of references**, even if only a single source field is used.

Two reference formats are permitted.

| Reference Type | Format | Example |
|---|---|---|
| Local schema property | `"#/properties/FieldName"` | `"#/properties/R0_Height20"` |
| SQL database source | `"Database.Table.Column"` | `"Mailing.General.ADOB"` |

#### Rules

- `x-derivedFrom` **must always be an array**.
- Use **JSON Pointer references** (`#/properties/...`) when referencing variables **within the same schema**.
- Use **`#/$defs/...` paths** when referencing variables defined inside `$defs`.
- Use **`Database.Table.Column`** format when the value is derived **directly from a SQL database source**.
- `x-derivedFrom` should only contain **machine-readable references**.  
  Explanatory details must be written in `x-description`.

---
### Example (Derived from other json schemas)


```json
"R0_BMI20": {
  "description": "BMI at 20 years old.",
  "oneOf": [
    {
      "type": "null"
    },
    {
      "const": 9999,
      "description": "Not applicable"
    },
    {
      "type": "number",
      "exclusiveMinimum": 0,
      "exclusiveMaximum": 165
    }
  ],
  "x-description": "Body Mass Index (kg/m²). Age calculation done by reading in age at baseline. Set to 9999 (NA) if participant was not yet 20 at baseline or if participant was pregnant at 20 years old.",
  "x-unit": "kg/m²",
  "x-derivedFrom": [
    "#/properties/R0_Height20",
    "#/properties/R0_Weight20",
    "#/$defs/AdminEvents/properties/Age_Entry",
    "#/$defs/AdminEvents/properties/DOB",
    "#/$defs/PregnanciesPII/properties/Pregnancies/R0_PregnancyEndDate",
    "#/$defs/PregnanciesPII/properties/Pregnancies/R0_Preg_DurationWks"
  ],
  "x-formerName": "bmi_20",
  "x-calculation": "R0_Weight20 / ((R0_Height20 / 100) ^ 2)"
}
```

#### Why this pattern is used

- `oneOf` distinguishes **three states**:
  - `null` → missing or invalid
  - `9999` → explicitly **not applicable**
  - numeric value → valid BMI
- `x-derivedFrom` documents the **exact fields used in derivation**.
- `x-calculation` records the **formula used to generate the variable**.
- `x-unit` ensures units are explicitly defined for numeric derived variables.
- `x-formerName` preserves compatibility with **previous variable naming conventions**.

### Example (Derived from SQL Database Sources)

```json
"AgeEntry": {
  "description": "Age at entry to study.",
  "type": ["integer", "null"],
  "x-description": "Using ADOB in the General table in Mailing database, and when the questionnaire was received, values of 6 in the Events table, to create Age at Entry. This variable is derived directly from raw data and the references to variables it is derived from are not another schema, they are references to the SQL database → table → column.",
  "x-derivedFrom": [
    "Mailing.General.ADOB",
    "Mailing.Events.Event",
    "Mailing.Events.Cancelled"
  ],
  "x-formerName": "age_entry",
  "minimum": 0,
  "x-unit": "years"
}
```

#### Explanation

- `x-derivedFrom` lists the **database sources used in the derivation**.
- Each entry follows the **`Database.Table.Column` format**.
- The detailed explanation of how the variable is constructed is placed in **`x-description`**.
- `x-unit` explicitly records the measurement unit.
- `minimum` ensures **invalid negative ages cannot pass validation**.
- `x-formerName` preserves the **previous variable name used in earlier datasets or scripts**.

---

## 11. additionalProperties

```json
"additionalProperties": false
```

- Prevents unrecognised fields from passing validation  
- Does not affect `x-*` custom keywords  

---

## 12. Versioning and provenance

1. Update `x-provenance.lastModified` to today (`YYYY-MM-DD`)  
2. Increment `x-version` according to change severity (MAJOR/MINOR/PATCH)  
3. If a field is renamed, add `x-formerName`  
4. If a field is removed, note it in PR, do **not** leave a stub  

---

## 13. Canonical $id URLs

```text
https://github.com/UK-Generations-Study/Schema_and_Derivation_Utils/tree/main/{Filename}.json
```

- `{Filename}` = filename or path  
- Examples:

```text
https://github.com/UK-Generations-Study/Schema_and_Derivation_Utils/blob/main/AdminEvents/schemas/AdminEvents_Schema.json
https://github.com/UK-Generations-Study/Schema_and_Derivation_Utils/blob/main/CancerSummary/json_schemas/CancerRegistry.json
```

- Do **not** use `http://`, alternate domains, path prefixes, or trailing `#`  

---

## 14. Common mistakes

| Mistake | Correction |
|---|---|
| `"type": ["string"]` | `"type": "string"` |
| `TCode` with `"type": ["string","null"]` | `"type": "string"` |
| TCode absent from `required` | Add `"TCode"` as first required entry |
| No `additionalProperties: false` | Add at root |
| Date field `"format": "date-time"` | Use `"format": "date"` for date-only |
| `_Shifted` field missing `"format"` | Always add `"format"` |
| `_Shifted` field missing `x-description` | Add standard privacy note |
| `x-enumDescriptions` has orphan code | Remove or add to `enum` |
| Grouped x-enumDescriptions | One per code only |
| `"name"` key in property | Remove if not applicable; key identity suffices |
| Non-standard root field | Use `x-provenance.dataRound` |
| `x-derivedFrom` narrative string | Must be an array |
| `x-description` duplicates `description` | Remove redundancy |
| Missing `x-version` or `x-provenance` | Add both |
| Age field missing upper bound | Add `"maximum": 120` only if known |
| Unit buried in description | Include `"unit"` key |

---

## 15. Validation

```bash
# Install validator (once)
pip install check-jsonschema

# Validate one schema
check-jsonschema --check-metaschema path/to/MySchema.json

# Validate all schemas in a directory
find . -name '*.json' | xargs check-jsonschema --check-metaschema
```

- Expected output: `ok -- validation done`  
- `fix_schemas.py` can audit and apply standards programmatically
