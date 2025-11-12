
import os
import sys
import pandas as pd

# -------------------------------------------------------------------
# 1. Paths – UPDATE THESE TO MATCH YOUR ENVIRONMENT
# -------------------------------------------------------------------
SCRIPTS_DIR = r"N:\CancerEpidem\BrBreakthrough\DeliveryProcess\Schema_and_Derivation_utils\Questionnaire\R0\scripts"
UTILS_DIR   = r"N:\CancerEpidem\BrBreakthrough\DeliveryProcess\Schema_and_Derivation_utils"

sys.path.append(os.path.abspath(SCRIPTS_DIR))
sys.path.append(os.path.abspath(UTILS_DIR))

# -------------------------------------------------------------------
# 2. Imports from your existing codebase
# -------------------------------------------------------------------
import cleaning_utils as cr
import nested_utils as nv

from common_utils import (
    load_and_pivot_data,
    load_schema,
    mask_pii,
    process_nested_data,
    extract_schema_constraints,
    init_varresolver_from_dfPII,
    save_change_tracking,
    validate_data,
    save_output
)

from restructure_utils import (
    restructure_by_schema,
    build_resolver_cache_from_columns,
    build_breast_cancer_resolver_cache
)

from pseudo_anon_utils import (
    apply_full_pseudo_anonymization,
    dateDict,
    update_schema
)

from qc_utils import (
    qc_check_variables,
    reconcile_value_frequencies
)

from schema_utils import build_variable_mapping

from utilities import createLogger

from config import (
    Delivery_log_path,
    test_server,
    r0_json_path,
    out_json_path,
    r0_json_path_pii,
    validation_path,
)

# -------------------------------------------------------------------
# 3. Sections to run (19 sections)
# -------------------------------------------------------------------
SECTION_ORDER = [
    "GeneralInformation",
    "BirthDetails",
    "PhysicalDevelopment",
    "Pregnancies",
    "MenstrualMenopause",
    "Mammograms",
    "AlcoholSmokingDiet",
    "XRays",
    "BreastDisease",
    "BreastCancer",
    "Jobs",
    "PhysicalActivity",
    "ContraceptiveHRT",
    "CancerRelatives",
    "MH_Illnesses",
    "MH_CancersBenignTumors",
    "MH_DrugsSupplements",
    "OtherBreastSurgery",
    "OtherLifestyleFactors"
]


# -------------------------------------------------------------------
# 4. Per-section runner (basically your load notebook as a function)
# -------------------------------------------------------------------
def run_section(q_sect: str):
    if q_sect not in dateDict:
        raise KeyError(f"{q_sect} not found in dateDict")

    # section slug used by nested_utils / restructure_utils
    section_slug = nv._SECTION_SLUGS[q_sect]

    # logger
    logger = createLogger(q_sect, Delivery_log_path)
    logger.info(f"=== Starting section: {q_sect} ({section_slug}) ===")

    # question range from dateDict
    question_range = dateDict[q_sect]["question_range"]

    # -------------------- Load & pivot --------------------
    pivoted, dfPII = load_and_pivot_data(question_range, logger)
    pivoted = pivoted.fillna("").reset_index()
    pivoted_dict = pivoted.set_index("StudyID").to_dict("index")

    # -------------------- Schema + var resolver --------------------
    schema = load_schema(r0_json_path, f"{q_sect}_Schema")
    init_varresolver_from_dfPII(dfPII, schema, q_sect)

    variable_mapping = build_variable_mapping(schema)

    constraint_map, var_type_map = extract_schema_constraints(schema)

    # -------------------- Cleaning & processing --------------------
    processed_data, change_tracking = process_nested_data(
        pivoted_dict,
        variable_mapping,
        var_type_map,
        constraint_map,
        cr.newValMap,
    )

    save_change_tracking(change_tracking, q_sect, logger)

    # -------------------- Restructure into nested JSON --------------------
    json_data = restructure_by_schema(
        processed_data,
        schema,
        section_slug,
        variable_mapping,
    )

    if q_sect == "BreastCancer":
        # build resolver cache for QC
        raw_df = pivoted.reset_index()
        resolver, res_path = build_breast_cancer_resolver_cache(
            q_sect, raw_df.columns
        )
    else:
        # build resolver cache for QC
        raw_df = pivoted.reset_index()
        resolver, res_path = build_resolver_cache_from_columns(
            section_slug, q_sect, raw_df.columns
        )
    logger.info(f"Resolver cache written to: {res_path}")

    # -------------------- Validate & save (allvar) --------------------
    validate_data(json_data, schema)
    save_output(json_data, f"{q_sect}_allvar", logger, stage="s1_allvar")

    # -------------------- Pseudo-anonymisation --------------------
    anon_data = apply_full_pseudo_anonymization(
        json_data,
        test_server,
        logger,
        schema,
        dateDict,
    )
    save_output(anon_data, f"{q_sect}_dateanon", logger, stage="s2_dateanon")

    # -------------------- PII masking --------------------
    pii_data, removed_pii_vars = mask_pii(anon_data, dfPII, schema)
    save_output(pii_data, f"{q_sect}_piimask", logger, stage="s3_piimask")

    # -------------------- PII schema (once per section) --------------------
    schema_pii_json = os.path.join(r0_json_path_pii, f"{q_sect}_Schema_PII.json")

    if not os.path.exists(schema_pii_json):
        update_schema(
            os.path.join(r0_json_path, f"{q_sect}_Schema.json"),
            schema_pii_json,
            dateDict,
            removed_pii_vars,
            q_sect,
        )

    schema_pii = load_schema(r0_json_path_pii, f"{q_sect}_Schema_PII")
    validate_data(pii_data, schema_pii, schema_pii_json)

    # -------------------- QC summaries --------------------
    summary_dir = os.path.join(validation_path, f"{q_sect}_ValidationSummary")
    os.makedirs(summary_dir, exist_ok=True)

    variable_report = qc_check_variables(
        raw_pivot_df=pivoted,
        processed_json=pii_data,
        resolver_index=resolver,
        dfPII=removed_pii_vars,
        section_name=q_sect,
        save_to=summary_dir,
    )

    value_recon = reconcile_value_frequencies(
        raw_pivot_df=pivoted,
        processed_json=pii_data,
        resolver_index=resolver,
        change_tracking=change_tracking,
        section_name=q_sect,
        schema=schema,
        variable_check=variable_report,
        save_to=summary_dir,
    )

    # Final anon drop without PII mask label
    save_output(anon_data, f"{q_sect}_anon", logger, stage="s4_anon")

    logger.info(f"=== Finished section: {q_sect} ===\n")


# -------------------------------------------------------------------
# 5. Main loop over all sections
# -------------------------------------------------------------------
if __name__ == "__main__":
    for q_sect in SECTION_ORDER:
        try:
            run_section(q_sect)
        except Exception as exc:
            print(f"[ERROR] Section {q_sect} failed: {exc}", file=sys.stderr)
