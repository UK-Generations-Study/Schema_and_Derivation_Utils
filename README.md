# Generations Study Data Processing Repository

This repository contains the schemas, scripts, and supporting materials used to process and structure data from the **Generations Study**, a large UK-based prospective cohort study investigating the causes of breast cancer.

The Generations Study follows more than **100,000 women across the UK**, collecting detailed information on lifestyle, medical history, genetics, and health outcomes. The goal is to improve understanding of breast cancer risk factors and support future prevention, diagnosis, and treatment research.

This repository supports the **ETL (Extract, Transform, Load) processes** used to transform raw study data into structured, validated datasets suitable for research and analysis. The codebase focuses on documenting the structure of the data and ensuring consistent, reproducible processing pipelines.

---

# Repository Overview

The repository is organised by **major study data domains**, with each module typically containing:

- **Schemas** – definitions of the expected data structure (often JSON schemas)
- **Scripts** – ETL and processing code used to transform and validate the data

```
├── AdminEvents
│   ├── schemas
│   └── scripts
├── CancerSummary
│   ├── json_schemas
│   └── scripts
├── Outcomes
│   ├── schemas
│   └── scripts
├── Pathology
│   ├── schemas
│   │   ├── pseudo_anon
│   │   └── raw
│   └── scripts
├── Questionnaire
│   └── R0
│       ├── schemas
│       │   ├── derived
│       │   ├── pseudo_anon
│       │   └── raw
│       │       └── archive
│       ├── scripts
│       └── validation

```

---

# Key Components

## Questionnaire

Contains the processing pipeline for questionnaire data collected from study participants.

This includes schema-driven ETL scripts, derived variables, and validation outputs for baseline questionnaire data.

## Pathology

Handles structured pathology data related to cancer diagnoses and tumour characteristics.

## Outcomes

Contains processing for study outcomes and follow-up information.

## CancerSummary

Provides harmonised summaries of cancer diagnoses used for downstream analysis.

## AdminEvents

Includes schemas and scripts related to administrative study events and participant tracking.

---

# Data Governance

This repository contains **code and schema definitions only**.

- No participant data is stored in this repository.
- Sensitive data is processed within secure environments in accordance with study governance policies and relevant data protection regulations (e.g., GDPR).

---

# Usage

The repository is intended for **authorised members of the Generations Study data engineering and research teams**. Each module may contain its own documentation describing specific pipelines and processing steps.
