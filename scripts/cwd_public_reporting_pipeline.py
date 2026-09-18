# =============================================================================
# Chronic Wasting Disease (CWD) Public Reporting ETL and Publishing Pipeline
# =============================================================================
#
# PURPOSE
# -------
# Prepare Chronic Wasting Disease (CWD) sample data for public reporting and
# maintain the Wildlife Management Unit (WMU) summary layer used by downstream
# ArcGIS Online maps, dashboards, and applications.
#
# The script reads the source CWD Excel dataset, performs data validation,
# normalization, and quality-control checks, exports a simplified public-facing
# dataset, calculates summary statistics by WMU, attaches those statistics to
# authoritative WMU polygons, and publishes or refreshes the resulting hosted
# feature layer in ArcGIS Online.
#
#
# MAIN WORKFLOW
# -------------
# 1. Read the source CWD Excel file and remove records with missing or duplicate
#    primary keys.
#
# 2. Validate Wildlife Management Unit (WMU) values against the authoritative
#    provincial WMU feature service. A WMU foreign key is assigned using
#    UPDATED_WMU where available, otherwise falling back to WMU.
#
# 3. Validate and normalize reporting fields, including:
#       - species
#       - CWD test status
#       - sample lifecycle dates
#       - submitter/source category
#       - sex
#       - test turnaround time
#
# 4. Produce QA/QC outputs describing issues or transformations, including:
#       - invalid or missing WMU values
#       - species values
#       - CWD status mappings
#       - date parsing and completeness
#
# 5. Export a reduced public-reporting Excel dataset containing only the fields
#    defined in PUBLICATION_FIELDS.
#
# 6. Aggregate the public dataset by WMU to calculate reporting statistics,
#    including:
#       - total samples collected
#       - current fiscal-year samples
#       - prior fiscal-year-to-date samples
#       - fiscal-year-over-year percentage change
#       - whether a positive CWD result has ever been recorded
#       - total positive samples and positive samples in the current fiscal year
#       - current-year and all-time sample counts by species and sex
#
# 7. Join the WMU summary statistics to authoritative WMU polygon geometry.
#    Geometry is queried in small batches to avoid performance problems caused
#    by requesting the full complex WMU dataset at once.
#
# 8. Publish the spatial WMU summary to ArcGIS Online:
#       - create the hosted feature layer if it does not already exist;
#       - recover and initialize a partially created empty service if necessary;
#       - otherwise replace the existing features while preserving the existing
#         AGOL item, service URL, and schema;
#       - share the resulting layer with the configured AGOL group.
#
#
# CONFIGURATION
# -------------
# Runtime configuration is read from environment variables. Local development
# may use a project .env file; GitHub Actions injects the same settings through
# workflow env/secrets. Source and Excel outputs use S3-compatible Object Storage.
#
#
# OUTPUTS
# -------
# Primary outputs:
#   - Public-facing CWD Excel dataset
#   - WMU summary Excel table
#   - Hosted ArcGIS Online WMU summary polygon layer
#
# QA/QC outputs written directly to the configured S3 output prefix:
#   - qa/wmu_failures.xlsx
#   - qa/species_summary.xlsx
#   - qa/status_summary.xlsx
#   - qa/date_summary.xlsx
#   - qa/date_completeness.xlsx
#   - wmu_summary.xlsx
#
# No Excel outputs are intentionally persisted in the repository/workspace.
#
#
# IMPORTANT BEHAVIOUR
# -------------------
# The existing hosted WMU summary layer is intentionally preserved between
# runs. When an existing layer is found, its features are replaced rather than
# publishing a new item. The script also validates the incoming dataframe
# against the existing layer schema and stops if unexpected fields would cause
# an unintended schema change.
#
# =============================================================================


# %%
from pathlib import Path
from io import BytesIO
import os
import logging
from typing import Iterable, Optional

import boto3
import pandas as pd
from botocore.config import Config as BotoConfig

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

from arcgis.gis import GIS
from arcgis.features import FeatureLayer, FeatureLayerCollection


# %% --------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"

# Local development can still use a .env file. GitHub Actions injects these
# values through the workflow environment instead.
if ENV_PATH.exists():
    if load_dotenv is None:
        raise ImportError(
            "A local .env file was found, but python-dotenv is not installed. "
            "Install python-dotenv for local .env support, or provide environment "
            "variables directly."
        )
    load_dotenv(ENV_PATH)


def require_env(name: str) -> str:
    """Return a required environment variable or raise a clear error."""
    value = os.getenv(name)
    if value is None or not value.strip():
        raise ValueError(f"Missing required environment variable: {name}")
    return value.strip()


def parse_sheet_name(value: str):
    value = str(value).strip()
    if value.isdigit():
        return int(value)
    return value


# -----------------------------------------------------------------------------
# Object Storage / S3
# -----------------------------------------------------------------------------

S3_ENDPOINT = require_env("S3_ENDPOINT")
S3_CWD_ACCESS_KEY = require_env("S3_CWD_ACCESS_KEY")
S3_CWD_SECRET_KEY = require_env("S3_CWD_SECRET_KEY")

# Keep bucket/key configuration in GitHub Actions variables rather than
# hard-coding internal storage locations in this public repository.
S3_INPUT_BUCKET = require_env("S3_INPUT_BUCKET")
S3_INPUT_KEY = require_env("S3_INPUT_KEY")

# Outputs default to the same bucket as the input unless explicitly overridden.
S3_OUTPUT_BUCKET = os.getenv("S3_OUTPUT_BUCKET") or S3_INPUT_BUCKET
S3_OUTPUT_PREFIX = (
    os.getenv("S3_OUTPUT_PREFIX")
    or "CWD_Reporting_Staging"
).strip("/")

QA_OUTPUT_PREFIX = (
    os.getenv("QA_OUTPUT_PREFIX")
    or f"{S3_OUTPUT_PREFIX}/qa"
).strip("/")

PUBLICATION_OUTPUT_KEY = (
    os.getenv("PUBLICATION_OUTPUT_KEY")
    or f"{S3_OUTPUT_PREFIX}/cwd_public_reporting.xlsx"
).strip("/")

WMU_SUMMARY_OUTPUT_KEY = (
    os.getenv("WMU_SUMMARY_OUTPUT_KEY")
    or f"{S3_OUTPUT_PREFIX}/wmu_summary.xlsx"
).strip("/")


# -----------------------------------------------------------------------------
# Excel / ETL configuration
# -----------------------------------------------------------------------------

SHEET_NAME_RAW = os.getenv("SHEET_NAME", "0")
SHEET_NAME = parse_sheet_name(SHEET_NAME_RAW)

PRIMARY_KEY = os.getenv("PRIMARY_KEY", "CWD_EAR_CARD_ID").strip()
if not PRIMARY_KEY:
    raise ValueError("PRIMARY_KEY cannot be blank.")


# -----------------------------------------------------------------------------
# Authoritative WMU service
# -----------------------------------------------------------------------------

WMU_LAYER_URL = (
    "https://services6.arcgis.com/ubm4tcTYICKBpist/"
    "arcgis/rest/services/WHSE_WILDLIFE__SimplifyPolyg1/FeatureServer/0"
)

VALID_WMU_FIELD = "WILDLIFE_MGMT_UNIT_ID"
WMU_FIELD = "WMU"
UPDATED_WMU_FIELD = "UPDATED_WMU"
WMU_FOREIGN_KEY_FIELD = "WMU_FOREIGN_KEY"

DATE_FIELDS = [
    "MORTALITY_DATE",
    "COLLECTION_DATE",
    "SAMPLED_DATE",
    "SAMPLE_DATE_SENT_TO_LAB",
    "REPORTING_LAB_DATE_RECEIVED",
    "CWD_TEST_STATUS_DATE",
]

PUBLICATION_FIELDS = [
    "CWD_EAR_CARD_ID",
    "WMU_FOREIGN_KEY",
    "SPECIES",
    "CWD_TEST_STATUS_PUBLIC",
    "MORTALITY_DATE",
    "COLLECTION_DATE",
    "SAMPLED_DATE",
    "SAMPLE_DATE_SENT_TO_LAB",
    "REPORTING_LAB_DATE_RECEIVED",
    "CWD_TEST_STATUS_DATE",
    "SEX",
    "DAYS_TO_TEST_STATUS",
    "SUBMITTER_SOURCE_CATEGORY",
]


# -----------------------------------------------------------------------------
# ArcGIS Online
# -----------------------------------------------------------------------------

# Support both the AGOL_* names used by this script and the AGO_* names already
# present in the existing GitHub Actions workflow.
AGOL_URL = (
    os.getenv("AGOL_URL")
    or os.getenv("AGO_HOST")
    or "https://www.arcgis.com"
)

AGOL_PROFILE = os.getenv("AGOL_PROFILE") or None

AGOL_USERNAME = (
    os.getenv("AGOL_USERNAME")
    or os.getenv("AGO_USERNAME")
    or None
)

AGOL_PASSWORD = (
    os.getenv("AGOL_PASSWORD")
    or os.getenv("AGO_PASSWORD")
    or None
)

WMU_SUMMARY_LAYER_TITLE = (
    os.getenv("WMU_SUMMARY_LAYER_TITLE")
    or "CWD WMU Summary by WMU"
)

WMU_SUMMARY_LAYER_TAGS = (
    os.getenv("WMU_SUMMARY_LAYER_TAGS")
    or "CWD,WMU,summary,wildlife"
)

WMU_SUMMARY_FOLDER = os.getenv("WMU_SUMMARY_FOLDER") or None
WMU_SUMMARY_ITEM_ID = os.getenv("WMU_SUMMARY_ITEM_ID") or None

# Intentionally no hard-coded group ID in the public script.
WMU_SUMMARY_GROUP_ID = os.getenv("WMU_SUMMARY_GROUP_ID") or None


SUMMARY_DATE_FIELD = "SAMPLED_DATE"
SUMMARY_STATUS_FIELD = "CWD_TEST_STATUS_PUBLIC"
SUMMARY_SPECIES_FIELD = "SPECIES"
SUMMARY_SEX_FIELD = "SEX"

SPECIES_SEX_OUTPUT_FIELDS = {
    ("Caribou", "Male"): "CY_CAR_M",
    ("Caribou", "Female"): "CY_CAR_F",
    ("Caribou", "Unknown"): "CY_CAR_U",

    ("Elk", "Male"): "CY_ELK_M",
    ("Elk", "Female"): "CY_ELK_F",
    ("Elk", "Unknown"): "CY_ELK_U",

    ("Moose", "Male"): "CY_MOOSE_M",
    ("Moose", "Female"): "CY_MOOSE_F",
    ("Moose", "Unknown"): "CY_MOOSE_U",

    ("Mule Deer", "Male"): "CY_MULE_M",
    ("Mule Deer", "Female"): "CY_MULE_F",
    ("Mule Deer", "Unknown"): "CY_MULE_U",

    ("White Tailed Deer", "Male"): "CY_WTD_M",
    ("White Tailed Deer", "Female"): "CY_WTD_F",
    ("White Tailed Deer", "Unknown"): "CY_WTD_U",

    ("Unknown", "Male"): "CY_UNK_M",
    ("Unknown", "Female"): "CY_UNK_F",
    ("Unknown", "Unknown"): "CY_UNK_U",
}

ALL_TIME_SPECIES_SEX_OUTPUT_FIELDS = {
    combo: field_name.replace("CY_", "AT_", 1)
    for combo, field_name in SPECIES_SEX_OUTPUT_FIELDS.items()
}

COUNT_FIELDS = [
    "TOTAL_ALL",
    "TOTAL_CUR_FY",
    "TOTAL_PRIOR_FYTD",
    "POS_ALL",
    "POS_CUR_FY",
    *SPECIES_SEX_OUTPUT_FIELDS.values(),
    *ALL_TIME_SPECIES_SEX_OUTPUT_FIELDS.values(),
]


# -----------------------------------------------------------------------------
# Explicit AGOL schema for WMU summary layer
# -----------------------------------------------------------------------------

WMU_SUMMARY_STRING_FIELDS = [
    VALID_WMU_FIELD,
    WMU_FOREIGN_KEY_FIELD,
    "POS_EVER_YN",
    "AS_OF_DATE",
    "CUR_FY_START",
    "PRIOR_FY_START",
    "PRIOR_FY_ASOF",
]

WMU_SUMMARY_INTEGER_FIELDS = [
    "TOTAL_ALL",
    "TOTAL_CUR_FY",
    "TOTAL_PRIOR_FYTD",
    "POS_ALL",
    "POS_CUR_FY",
    *SPECIES_SEX_OUTPUT_FIELDS.values(),
    *ALL_TIME_SPECIES_SEX_OUTPUT_FIELDS.values(),
]

WMU_SUMMARY_DOUBLE_FIELDS = [
    "PCT_CHG_FYTD",
]

WMU_SUMMARY_EXPECTED_FIELDS = (
    WMU_SUMMARY_STRING_FIELDS
    + WMU_SUMMARY_INTEGER_FIELDS
    + WMU_SUMMARY_DOUBLE_FIELDS
)


# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# General ETL helpers
# -----------------------------------------------------------------------------

def get_s3_client():
    """Create an authenticated S3-compatible Object Storage client."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_CWD_ACCESS_KEY,
        aws_secret_access_key=S3_CWD_SECRET_KEY,
        config=BotoConfig(
            retries={
                "max_attempts": 10,
                "mode": "standard",
            }
        ),
    )


def read_excel_from_s3(
    s3_client,
    bucket: str,
    key: str,
    sheet_name=0,
) -> pd.DataFrame:
    """
    Read an Excel workbook directly from Object Storage into memory.

    No copy is written to the GitHub Actions workspace.
    """
    logger.info("Reading configured master Excel workbook from Object Storage.")

    response = s3_client.get_object(
        Bucket=bucket,
        Key=key,
    )

    file_bytes = response["Body"].read()

    logger.info(f"Downloaded {len(file_bytes):,} bytes from Object Storage.")

    df = pd.read_excel(
        BytesIO(file_bytes),
        sheet_name=sheet_name,
        engine="openpyxl",
        converters={PRIMARY_KEY: str},
    )

    logger.info(f"Loaded {len(df):,} rows and {len(df.columns):,} columns.")

    return df


def save_dataframe_to_s3_excel(
    s3_client,
    df: pd.DataFrame,
    bucket: str,
    key: str,
    sheet_name: str = "Sheet1",
) -> None:
    """
    Write a dataframe to an in-memory XLSX buffer and upload it to Object Storage.

    The workbook is never written to the repository or GitHub Actions workspace.
    """
    buffer = BytesIO()

    df.to_excel(
        buffer,
        sheet_name=sheet_name,
        index=False,
        engine="openpyxl",
    )

    buffer.seek(0)

    payload = buffer.getvalue()

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=payload,
        ContentType=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
    )

    logger.info(
        f"Excel output uploaded to Object Storage "
        f"({len(payload):,} bytes)."
    )


def qa_output_key(filename: str) -> str:
    """Build an S3 key beneath the configured QA output prefix."""
    return f"{QA_OUTPUT_PREFIX}/{filename}"


def clean_primary_key(
    df: pd.DataFrame,
    primary_key: str,
) -> pd.DataFrame:
    df = df.copy()

    if primary_key not in df.columns:
        logger.error(f"Primary key field not found: {primary_key}")
        return df

    starting_rows = len(df)

    df[primary_key] = df[primary_key].astype("string").str.strip()

    blank_mask = df[primary_key].isna() | df[primary_key].eq("")
    blank_count = int(blank_mask.sum())

    if blank_count > 0:
        logger.warning(
            f"Removing {blank_count:,} records with blank/null "
            f"'{primary_key}' values."
        )
        df = df.loc[~blank_mask].copy()

    duplicate_mask = df[primary_key].duplicated(keep="first")
    duplicate_count = int(duplicate_mask.sum())

    if duplicate_count > 0:
        logger.warning(
            f"Removing {duplicate_count:,} duplicate records based on "
            f"'{primary_key}', keeping the first instance."
        )
        df = df.loc[~duplicate_mask].copy()

    ending_rows = len(df)

    logger.info(
        "Primary key cleanup complete. "
        f"Started with {starting_rows:,} rows; ended with {ending_rows:,} rows; "
        f"removed {starting_rows - ending_rows:,} rows."
    )

    return df


def normalize_wmu_value(value) -> str:
    if pd.isna(value):
        return ""

    value = str(value).strip()

    if value.lower() in {"nan", "none", "null"}:
        return ""

    if value.endswith(".0"):
        value = value[:-2]

    return value


# -----------------------------------------------------------------------------
# WMU lookup and validation
# -----------------------------------------------------------------------------

def get_valid_wmu_values(
    layer_url: str = WMU_LAYER_URL,
    valid_field: str = VALID_WMU_FIELD,
    agol_url: str = AGOL_URL,
    agol_username: str = AGOL_USERNAME,
    agol_password: str = AGOL_PASSWORD,
) -> list[str]:
    logger.info(f"Querying valid WMU values from: {layer_url}")

    if not agol_username or not agol_password:
        raise ValueError(
            "ArcGIS Online username/password are required to access the WMU layer."
        )

    logger.info("Connecting to ArcGIS Online with configured credentials.")

    gis = GIS(
        url=agol_url,
        username=agol_username,
        password=agol_password,
    )

    if gis.users.me is None:
        raise RuntimeError("ArcGIS Online authentication failed.")

    logger.info("Successfully connected to ArcGIS Online.")

    layer = FeatureLayer(
        layer_url,
        gis=gis,
    )

    result_df = layer.query(
        where="1=1",
        out_fields=valid_field,
        return_geometry=False,
        return_distinct_values=True,
        return_all_records=True,
        as_df=True,
    )

    if result_df is None or len(result_df) == 0:
        logger.error("WMU lookup returned zero records.")
        return []

    if valid_field not in result_df.columns:
        logger.error(
            f"Expected field '{valid_field}' was not found in WMU lookup. "
            f"Returned fields were: {list(result_df.columns)}"
        )
        return []

    valid_values = sorted(
        {
            normalize_wmu_value(value)
            for value in result_df[valid_field]
            if normalize_wmu_value(value) != ""
        }
    )

    if len(valid_values) == 0:
        logger.error(
            "WMU lookup completed, but produced zero valid WMU values."
        )
    else:
        logger.info(
            f"Loaded {len(valid_values):,} valid WMU values."
        )

    return valid_values

def assign_wmu_foreign_key(
    df: pd.DataFrame,
    valid_wmu_values: list[str],
    wmu_field: str = WMU_FIELD,
    updated_wmu_field: str = UPDATED_WMU_FIELD,
    output_field: str = WMU_FOREIGN_KEY_FIELD,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    issues = []

    required_fields = [wmu_field, updated_wmu_field]

    for field in required_fields:
        if field not in df.columns:
            logger.error(f"Required WMU field is missing: {field}")
            issues.append(
                {
                    "row_index": None,
                    "field": field,
                    "issue_type": "missing_required_field",
                    "issue_detail": f"Required field '{field}' was not found in dataframe.",
                }
            )
            return df, pd.DataFrame(issues)

    valid_wmu_set = {
        normalize_wmu_value(value)
        for value in valid_wmu_values
        if normalize_wmu_value(value) != ""
    }

    if len(valid_wmu_set) == 0:
        logger.error("Valid WMU list is empty. WMU validation cannot be performed.")
        issues.append(
            {
                "row_index": None,
                "field": VALID_WMU_FIELD,
                "issue_type": "empty_validation_list",
                "issue_detail": "Valid WMU lookup list was empty.",
            }
        )
        return df, pd.DataFrame(issues)

    clean_wmu = df[wmu_field].apply(normalize_wmu_value)
    clean_updated_wmu = df[updated_wmu_field].apply(normalize_wmu_value)

    df[output_field] = clean_updated_wmu.where(
        clean_updated_wmu != "",
        clean_wmu,
    )

    df["WMU_FOREIGN_KEY_SOURCE"] = updated_wmu_field
    df.loc[clean_updated_wmu == "", "WMU_FOREIGN_KEY_SOURCE"] = wmu_field

    both_blank_mask = (clean_wmu == "") & (clean_updated_wmu == "")

    if both_blank_mask.any():
        count = int(both_blank_mask.sum())
        logger.warning(
            f"{count:,} records have both '{wmu_field}' and "
            f"'{updated_wmu_field}' blank."
        )

        for row_index in df.index[both_blank_mask]:
            issues.append(
                {
                    "row_index": row_index,
                    "field": output_field,
                    "issue_type": "missing_wmu_foreign_key",
                    "issue_detail": (
                        f"Both '{wmu_field}' and '{updated_wmu_field}' are blank."
                    ),
                }
            )

    invalid_mask = (
        ~both_blank_mask
        & ~df[output_field].isin(valid_wmu_set)
    )

    if invalid_mask.any():
        count = int(invalid_mask.sum())
        logger.warning(
            f"{count:,} records have a selected WMU value that is not in "
            "the authoritative WMU lookup list."
        )

        for row_index, value in df.loc[invalid_mask, output_field].items():
            issues.append(
                {
                    "row_index": row_index,
                    "field": output_field,
                    "issue_type": "invalid_wmu_foreign_key",
                    "issue_detail": f"Selected WMU value is not valid: {value}",
                }
            )

    df["WMU_FOREIGN_KEY_IS_VALID"] = df[output_field].isin(valid_wmu_set)
    df.loc[both_blank_mask, "WMU_FOREIGN_KEY_IS_VALID"] = False

    df["WMU_FOREIGN_KEY_VALIDATION_NOTE"] = ""

    df.loc[
        both_blank_mask,
        "WMU_FOREIGN_KEY_VALIDATION_NOTE",
    ] = f"Both {wmu_field} and {updated_wmu_field} are blank."

    df.loc[
        invalid_mask,
        "WMU_FOREIGN_KEY_VALIDATION_NOTE",
    ] = "Selected WMU value is not in authoritative lookup list."

    if not issues:
        logger.info("WMU foreign-key validation passed.")

    issues_df = pd.DataFrame(issues)

    return df, issues_df


# -----------------------------------------------------------------------------
# Field validation and normalization
# -----------------------------------------------------------------------------

def validate_species_field(
    df: pd.DataFrame,
    species_field: str = "SPECIES",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()

    if species_field not in df.columns:
        logger.warning(f"Species field not found: {species_field}")

        species_summary_df = pd.DataFrame(
            [
                {
                    "field": species_field,
                    "issue": "missing_field",
                    "detail": f"Field '{species_field}' was not found in dataframe.",
                }
            ]
        )

        return df, species_summary_df

    df[species_field] = df[species_field].astype("string").str.strip()

    blank_mask = df[species_field].eq("")
    null_mask = df[species_field].isna()
    missing_mask = blank_mask | null_mask

    missing_count = int(missing_mask.sum())

    if missing_count > 0:
        logger.warning(
            f"Species field contains {missing_count:,} blank/null values."
        )
    else:
        logger.info("Species field contains no blank/null values.")

    species_summary_df = (
        df[species_field]
        .fillna("<NULL>")
        .replace("", "<BLANK>")
        .value_counts(dropna=False)
        .rename_axis(species_field)
        .reset_index(name="record_count")
    )

    logger.info(f"Species field contains {len(species_summary_df):,} unique values.")

    logger.info(
        "Species values found: "
        + ", ".join(species_summary_df[species_field].astype(str).tolist())
    )

    return df, species_summary_df


def simplify_cwd_test_status(
    df: pd.DataFrame,
    source_field: str = "CWD_TEST_STATUS",
    output_field: str = "CWD_TEST_STATUS_PUBLIC",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()

    if source_field not in df.columns:
        logger.warning(f"Field not found: {source_field}")

        summary_df = pd.DataFrame(
            [
                {
                    "source_field": source_field,
                    "issue": "missing_field",
                    "detail": f"Field '{source_field}' was not found in dataframe.",
                }
            ]
        )

        return df, summary_df

    clean_status = df[source_field].astype("string").str.strip()

    status_map = {
        "Negative": "Negative",
        "negative": "Negative",

        "Positive": "Positive",
        "positive": "Positive",
        "Postive": "Positive",
        "postive": "Positive",

        "Pending": "Pending",
        "pending": "Pending",

        "Not Tested": "Not Tested / No Result",
        "Not tested": "Not Tested / No Result",
        "not tested": "Not Tested / No Result",

        "Unsuitable Tissue": "Not Tested / No Result",
        "unsuitable tissue": "Not Tested / No Result",

        "-": "Not Tested / No Result",
        "": "Not Tested / No Result",
    }

    df[output_field] = clean_status.map(status_map).fillna("Not Tested / No Result")

    summary_df = (
        pd.DataFrame(
            {
                source_field: clean_status.fillna("<NULL>").replace("", "<BLANK>"),
                output_field: df[output_field],
            }
        )
        .value_counts()
        .reset_index(name="record_count")
        .sort_values([output_field, source_field])
    )

    logger.info(f"Simplified '{source_field}' into '{output_field}'.")

    logger.info(
        "Public CWD status values: "
        + ", ".join(sorted(df[output_field].dropna().unique()))
    )

    return df, summary_df


def standardize_date_fields(
    df: pd.DataFrame,
    date_fields: list[str],
    output_format: str = "%Y-%m-%d",
    overwrite: bool = True,
    date_count_field: str = "CWD_SAMPLE_LIFECYCLE_DATE_COUNT",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    summary_records = []
    parsed_date_lookup = {}

    for field in date_fields:
        if field not in df.columns:
            logger.warning(f"Date field not found: {field}")

            summary_records.append(
                {
                    "field": field,
                    "status": "missing_field",
                    "total_records": len(df),
                    "original_blank_count": None,
                    "parsed_date_count": None,
                    "invalid_date_count": None,
                    "min_date": None,
                    "max_date": None,
                }
            )

            parsed_date_lookup[field] = pd.Series(pd.NaT, index=df.index)
            continue

        original_values = df[field]

        original_blank_mask = (
            original_values.isna()
            | original_values.astype("string").str.strip().eq("")
        )

        original_blank_count = int(original_blank_mask.sum())

        parsed_dates = pd.to_datetime(
            original_values,
            errors="coerce",
        )

        parsed_date_lookup[field] = parsed_dates

        invalid_mask = parsed_dates.isna() & ~original_blank_mask
        invalid_count = int(invalid_mask.sum())

        if invalid_count > 0:
            logger.warning(
                f"Field '{field}' contains {invalid_count:,} invalid date values."
            )

            invalid_examples = (
                original_values[invalid_mask]
                .astype("string")
                .dropna()
                .unique()
                [:10]
            )

            logger.warning(
                f"Example invalid values for '{field}': {list(invalid_examples)}"
            )

        parsed_count = int(parsed_dates.notna().sum())

        logger.info(
            f"Date field '{field}': "
            f"{parsed_count:,} parsed, "
            f"{original_blank_count:,} blank, "
            f"{invalid_count:,} invalid."
        )

        standardized_dates = parsed_dates.dt.strftime(output_format).fillna("")

        if overwrite:
            output_field = field
        else:
            output_field = f"{field}_STANDARDIZED"

        df[output_field] = standardized_dates

        summary_records.append(
            {
                "field": field,
                "status": "processed",
                "total_records": len(df),
                "original_blank_count": original_blank_count,
                "parsed_date_count": parsed_count,
                "invalid_date_count": invalid_count,
                "min_date": (
                    parsed_dates.min().strftime(output_format)
                    if parsed_dates.notna().any()
                    else None
                ),
                "max_date": (
                    parsed_dates.max().strftime(output_format)
                    if parsed_dates.notna().any()
                    else None
                ),
            }
        )

    date_summary_df = pd.DataFrame(summary_records)

    parsed_dates_df = pd.DataFrame(parsed_date_lookup, index=df.index)

    df[date_count_field] = parsed_dates_df.notna().sum(axis=1)

    max_date_count = len(date_fields)

    date_completeness_summary_df = (
        df[date_count_field]
        .value_counts()
        .reindex(range(max_date_count, -1, -1), fill_value=0)
        .rename_axis("valid_date_count")
        .reset_index(name="record_count")
    )

    date_completeness_summary_df["total_possible_date_fields"] = max_date_count

    date_completeness_summary_df["description"] = (
        date_completeness_summary_df["valid_date_count"].astype(str)
        + " of "
        + str(max_date_count)
        + " lifecycle date fields populated"
    )

    logger.info("Lifecycle date completeness summary:")

    for _, row in date_completeness_summary_df.iterrows():
        logger.info(
            f"{row['valid_date_count']} of {max_date_count} dates populated: "
            f"{row['record_count']:,} records"
        )

    return df, date_summary_df, date_completeness_summary_df


def calculate_test_turnaround_days(
    df: pd.DataFrame,
    sampled_date_field: str = "SAMPLED_DATE",
    status_date_field: str = "CWD_TEST_STATUS_DATE",
) -> pd.DataFrame:
    result = df.copy()

    sampled_date = pd.to_datetime(
        result[sampled_date_field],
        format="%Y-%m-%d",
        errors="coerce",
    )

    status_date = pd.to_datetime(
        result[status_date_field],
        format="%Y-%m-%d",
        errors="coerce",
    )

    both_dates_present = sampled_date.notna() & status_date.notna()
    invalid_date_order = both_dates_present & (status_date < sampled_date)

    day_difference = (status_date - sampled_date).dt.days

    result["DAYS_TO_TEST_STATUS"] = day_difference.where(
        both_dates_present & ~invalid_date_order
    ).astype("Int64")

    result["DATE_ERROR_CODE"] = pd.Series(pd.NA, index=result.index, dtype="string")
    result.loc[invalid_date_order, "DATE_ERROR_CODE"] = (
        "ERR_TEST_STATUS_BEFORE_SAMPLE"
    )

    return result


def normalize_sample_source(
    df: pd.DataFrame,
    sample_source_field: str = "SUBMITTER_SOURCE_CATEGORY",
) -> pd.DataFrame:
    result = df.copy()

    hunter_list = ["Hunter"]
    motor_vehicle_list = ["Highway Crew"]

    hunter_mask = result[sample_source_field].isin(hunter_list)
    motor_vehicle_mask = result[sample_source_field].isin(motor_vehicle_list)

    result.loc[hunter_mask, sample_source_field] = "Hunter"
    result.loc[motor_vehicle_mask, sample_source_field] = "Motor Vehicle Strike"
    result.loc[
        ~(hunter_mask | motor_vehicle_mask),
        sample_source_field,
    ] = "Other"

    return result


def normalize_species(
    df: pd.DataFrame,
    species_field: str = "SPECIES",
) -> pd.DataFrame:
    result = df.copy()

    species_mapping = {
        "Black Tail Deer": "Mule Deer",
        "Mule Deer": "Mule Deer",

        "Rocky Mountain Elk": "Elk",
        "Roosevelt Elk": "Elk",
        "Elk": "Elk",

        "Caribou": "Caribou",
        "White Tailed Deer": "White Tailed Deer",
        "Moose": "Moose",

        "Unknown": "Unknown",
        "Unkown": "Unknown",
    }

    result[species_field] = (
        result[species_field]
        .astype("string")
        .str.strip()
        .map(species_mapping)
        .fillna("Unknown")
    )

    return result


def normalize_sex_value(value) -> str:
    if pd.isna(value):
        return "Unknown"

    value = str(value).strip()

    sex_map = {
        "male": "Male",
        "m": "Male",
        "female": "Female",
        "f": "Female",
        "unknown": "Unknown",
        "unkown": "Unknown",
        "nan": "Unknown",
        "none": "Unknown",
        "null": "Unknown",
        "": "Unknown",
    }

    return sex_map.get(value.lower(), "Unknown")


def export_publication_excel(
    df: pd.DataFrame,
    s3_client,
    bucket: str = S3_OUTPUT_BUCKET,
    key: str = PUBLICATION_OUTPUT_KEY,
    publication_fields: list[str] = PUBLICATION_FIELDS,
    sheet_name: str = "CWD_Public_Data",
) -> pd.DataFrame:
    """
    Build the reduced public-reporting dataframe and write it directly to S3.
    """
    df = df.copy()

    missing_fields = [
        field for field in publication_fields
        if field not in df.columns
    ]

    if missing_fields:
        logger.error(
            f"Cannot export. Missing required publication fields: {missing_fields}"
        )
        raise ValueError(
            f"Missing required publication fields: {missing_fields}"
        )

    publication_df = df[publication_fields].copy()

    logger.info(
        f"Exporting publication dataframe with "
        f"{len(publication_df):,} rows and "
        f"{len(publication_df.columns):,} columns."
    )

    save_dataframe_to_s3_excel(
        s3_client=s3_client,
        df=publication_df,
        bucket=bucket,
        key=key,
        sheet_name=sheet_name,
    )

    return publication_df


# -----------------------------------------------------------------------------
# WMU summary table
# -----------------------------------------------------------------------------

def get_fiscal_periods(
    as_of=None,
) -> tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp, pd.Timestamp]:
    if as_of is None:
        as_of = pd.Timestamp.today().normalize()
    else:
        as_of = pd.Timestamp(as_of).normalize()

    fiscal_start_year = as_of.year if as_of.month >= 4 else as_of.year - 1

    current_fy_start = pd.Timestamp(fiscal_start_year, 4, 1)
    current_fy_as_of = as_of

    prior_fy_start = pd.Timestamp(fiscal_start_year - 1, 4, 1)
    prior_fy_same_as_of = as_of - pd.DateOffset(years=1)

    return current_fy_start, current_fy_as_of, prior_fy_start, prior_fy_same_as_of


def make_wmu_summary(
    df: pd.DataFrame,
    as_of=None,
) -> pd.DataFrame:
    result = df.copy()

    current_start, current_end, prior_start, prior_end = get_fiscal_periods(as_of)

    # -------------------------------------------------------------------------
    # Normalize summary fields
    # -------------------------------------------------------------------------

    result[WMU_FOREIGN_KEY_FIELD] = (
        result[WMU_FOREIGN_KEY_FIELD]
        .apply(normalize_wmu_value)
        .replace("", "Not Recorded")
    )

    result[SUMMARY_DATE_FIELD] = pd.to_datetime(
        result[SUMMARY_DATE_FIELD],
        errors="coerce",
    ).dt.normalize()

    result[SUMMARY_STATUS_FIELD] = (
        result[SUMMARY_STATUS_FIELD]
        .astype("string")
        .str.strip()
    )

    result[SUMMARY_SPECIES_FIELD] = (
        result[SUMMARY_SPECIES_FIELD]
        .astype("string")
        .str.strip()
        .fillna("Unknown")
    )

    result[SUMMARY_SEX_FIELD] = (
        result[SUMMARY_SEX_FIELD]
        .apply(normalize_sex_value)
    )

    # -------------------------------------------------------------------------
    # Reporting-period masks
    # -------------------------------------------------------------------------

    current_fy_mask = result[SUMMARY_DATE_FIELD].between(
        current_start,
        current_end,
        inclusive="both",
    )

    prior_fy_same_point_mask = result[SUMMARY_DATE_FIELD].between(
        prior_start,
        prior_end,
        inclusive="both",
    )

    positive_mask = (
        result[SUMMARY_STATUS_FIELD]
        .str.casefold()
        .eq("positive")
    )

    # -------------------------------------------------------------------------
    # WMU index
    # -------------------------------------------------------------------------

    all_wmus = pd.Index(
        sorted(result[WMU_FOREIGN_KEY_FIELD].dropna().unique()),
        name=WMU_FOREIGN_KEY_FIELD,
    )

    if "Not Recorded" not in all_wmus:
        all_wmus = all_wmus.union(
            pd.Index(
                ["Not Recorded"],
                name=WMU_FOREIGN_KEY_FIELD,
            )
        )

    # -------------------------------------------------------------------------
    # Overall sample counts
    # -------------------------------------------------------------------------

    total_all = (
        result
        .groupby(WMU_FOREIGN_KEY_FIELD)
        .size()
        .reindex(all_wmus, fill_value=0)
        .rename("TOTAL_ALL")
    )

    total_current_fy = (
        result.loc[current_fy_mask]
        .groupby(WMU_FOREIGN_KEY_FIELD)
        .size()
        .reindex(all_wmus, fill_value=0)
        .rename("TOTAL_CUR_FY")
    )

    total_prior_fytd = (
        result.loc[prior_fy_same_point_mask]
        .groupby(WMU_FOREIGN_KEY_FIELD)
        .size()
        .reindex(all_wmus, fill_value=0)
        .rename("TOTAL_PRIOR_FYTD")
    )

    pct_change = (
        (total_current_fy - total_prior_fytd)
        / total_prior_fytd.replace(0, pd.NA)
        * 100
    ).rename("PCT_CHG_FYTD")

    # -------------------------------------------------------------------------
    # Positive CWD results
    # -------------------------------------------------------------------------

    positive_ever = (
        result.assign(_positive=positive_mask)
        .groupby(WMU_FOREIGN_KEY_FIELD)["_positive"]
        .any()
        .reindex(all_wmus, fill_value=False)
        .map({True: "Yes", False: "No"})
        .rename("POS_EVER_YN")
    )

    positive_all_time = (
        result.loc[positive_mask]
        .groupby(WMU_FOREIGN_KEY_FIELD)
        .size()
        .reindex(all_wmus, fill_value=0)
        .rename("POS_ALL")
    )

    positive_current_fy = (
        result.loc[current_fy_mask & positive_mask]
        .groupby(WMU_FOREIGN_KEY_FIELD)
        .size()
        .reindex(all_wmus, fill_value=0)
        .rename("POS_CUR_FY")
    )

    # -------------------------------------------------------------------------
    # Species x sex matrices
    # -------------------------------------------------------------------------

    desired_combo_columns = pd.MultiIndex.from_tuples(
        list(SPECIES_SEX_OUTPUT_FIELDS.keys()),
        names=[SUMMARY_SPECIES_FIELD, SUMMARY_SEX_FIELD],
    )

    current_fy_combo_counts = pd.crosstab(
        index=result.loc[current_fy_mask, WMU_FOREIGN_KEY_FIELD],
        columns=[
            result.loc[current_fy_mask, SUMMARY_SPECIES_FIELD],
            result.loc[current_fy_mask, SUMMARY_SEX_FIELD],
        ],
    )

    current_fy_combo_counts = current_fy_combo_counts.reindex(
        index=all_wmus,
        columns=desired_combo_columns,
        fill_value=0,
    )

    current_fy_combo_counts.columns = [
        SPECIES_SEX_OUTPUT_FIELDS[col]
        for col in current_fy_combo_counts.columns
    ]

    all_time_combo_counts = pd.crosstab(
        index=result[WMU_FOREIGN_KEY_FIELD],
        columns=[
            result[SUMMARY_SPECIES_FIELD],
            result[SUMMARY_SEX_FIELD],
        ],
    )

    all_time_combo_counts = all_time_combo_counts.reindex(
        index=all_wmus,
        columns=desired_combo_columns,
        fill_value=0,
    )

    all_time_combo_counts.columns = [
        ALL_TIME_SPECIES_SEX_OUTPUT_FIELDS[col]
        for col in all_time_combo_counts.columns
    ]

    # -------------------------------------------------------------------------
    # Assemble summary
    # -------------------------------------------------------------------------

    summary = pd.concat(
        [
            total_all,
            total_current_fy,
            total_prior_fytd,
            pct_change,
            positive_ever,
            positive_all_time,
            positive_current_fy,
            current_fy_combo_counts,
            all_time_combo_counts,
        ],
        axis=1,
    ).reset_index()

    summary["AS_OF_DATE"] = current_end.strftime("%Y-%m-%d")
    summary["CUR_FY_START"] = current_start.strftime("%Y-%m-%d")
    summary["PRIOR_FY_START"] = prior_start.strftime("%Y-%m-%d")
    summary["PRIOR_FY_ASOF"] = prior_end.strftime("%Y-%m-%d")

    for field in COUNT_FIELDS:
        if field in summary.columns:
            summary[field] = summary[field].fillna(0).astype(int)

    return summary


# -----------------------------------------------------------------------------
# Spatialize summary onto WMU polygons
# -----------------------------------------------------------------------------

def sql_quote(value: str) -> str:
    """
    Quote a value for a simple ArcGIS SQL IN clause.
    Escapes single quotes defensively.
    """
    return "'" + str(value).replace("'", "''") + "'"


def make_in_clauses(
    field_name: str,
    values: list[str],
    chunk_size: int = 50,
) -> list[str]:
    """
    Build chunked SQL IN clauses.
    """
    clean_values = [
        normalize_wmu_value(value)
        for value in values
        if normalize_wmu_value(value) not in {"", "Not Recorded"}
    ]

    clean_values = sorted(set(clean_values))

    clauses = []

    for value_chunk in chunk_list(clean_values, chunk_size):
        quoted_values = ", ".join(sql_quote(value) for value in value_chunk)
        clauses.append(f"{field_name} IN ({quoted_values})")

    return clauses


def get_object_ids_for_where(layer: FeatureLayer, where: str) -> list[int]:
    """
    Query object IDs only.
    """
    result = layer.query(
        where=where,
        return_ids_only=True,
    )

    if isinstance(result, dict):
        object_ids = result.get("objectIds", [])
    else:
        # Some ArcGIS API versions may return a FeatureSet-like object.
        object_ids = getattr(result, "object_ids", [])

    if object_ids is None:
        return []

    return sorted(object_ids)


def query_features_by_object_ids(
    layer: FeatureLayer,
    object_ids: list[int],
    out_fields: str,
    batch_size: int = 5,
) -> pd.DataFrame:
    """
    Query features with geometry in small object-id batches.

    Batch size is intentionally small because WMU polygons can be complex.
    """

    dfs = []

    for oid_chunk in chunk_list(object_ids, batch_size):
        logger.info(f"Querying {len(oid_chunk):,} WMU geometries.")

        chunk_sdf = layer.query(
            object_ids=",".join(str(oid) for oid in oid_chunk),
            out_fields=out_fields,
            return_geometry=True,
            return_all_records=False,
            as_df=True,
        )

        if chunk_sdf is not None and len(chunk_sdf) > 0:
            dfs.append(chunk_sdf)

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


def build_wmu_summary_spatial_dataframe(
    summary_df: pd.DataFrame,
    wmu_layer_url: str = WMU_LAYER_URL,
    valid_wmu_field: str = VALID_WMU_FIELD,
) -> pd.DataFrame:
    """
    Materialize the WMU summary onto authoritative WMU geometries.

    This version avoids querying all geometries at once. It:
    1. gets the WMU keys needed from the summary table;
    2. queries object IDs only;
    3. fetches geometries in small batches;
    4. joins the pandas summary onto the spatial dataframe.
    """

    logger.info("Querying authoritative WMU polygons for spatial summary layer.")

    wmu_layer = FeatureLayer(wmu_layer_url)

    summary_for_join = summary_df.copy()

    summary_for_join["WMU_JOIN_KEY"] = summary_for_join[WMU_FOREIGN_KEY_FIELD].apply(
        normalize_wmu_value
    )

    not_recorded_mask = summary_for_join["WMU_JOIN_KEY"].eq("Not Recorded")

    if not_recorded_mask.any():
        logger.warning(
            "The summary contains a 'Not Recorded' row. "
            "It will not be included in the spatial polygon layer because it has no WMU geometry."
        )

    summary_for_join = summary_for_join.loc[~not_recorded_mask].copy()

    needed_wmus = sorted(summary_for_join["WMU_JOIN_KEY"].dropna().unique())

    if not needed_wmus:
        raise ValueError("No valid WMU values were available for spatialization.")

    logger.info(f"Preparing to query geometries for {len(needed_wmus):,} WMUs.")

    object_ids = []

    for where_clause in make_in_clauses(
        field_name=valid_wmu_field,
        values=needed_wmus,
        chunk_size=50,
    ):
        ids = get_object_ids_for_where(wmu_layer, where_clause)
        object_ids.extend(ids)

    object_ids = sorted(set(object_ids))

    if not object_ids:
        raise ValueError(
            "No WMU polygon object IDs were returned from the authoritative WMU layer."
        )

    logger.info(f"Found {len(object_ids):,} WMU polygon object IDs.")

    wmu_sdf = query_features_by_object_ids(
        layer=wmu_layer,
        object_ids=object_ids,
        out_fields=valid_wmu_field,
        batch_size=25,
    )

    if wmu_sdf is None or len(wmu_sdf) == 0:
        raise ValueError("WMU polygon layer query returned no geometry records.")

    if valid_wmu_field not in wmu_sdf.columns:
        raise ValueError(
            f"Expected WMU field '{valid_wmu_field}' not found in WMU polygon layer."
        )

    geometry_field = wmu_sdf.spatial.name

    wmu_sdf = wmu_sdf[[valid_wmu_field, geometry_field]].copy()
    wmu_sdf["WMU_JOIN_KEY"] = wmu_sdf[valid_wmu_field].apply(normalize_wmu_value)

    spatial_summary_sdf = wmu_sdf.merge(
        summary_for_join.drop(columns=[valid_wmu_field], errors="ignore"),
        how="left",
        on="WMU_JOIN_KEY",
    )

    spatial_summary_sdf[WMU_FOREIGN_KEY_FIELD] = spatial_summary_sdf["WMU_JOIN_KEY"]

    for field in COUNT_FIELDS:
        if field in spatial_summary_sdf.columns:
            spatial_summary_sdf[field] = (
                spatial_summary_sdf[field]
                .fillna(0)
                .astype(int)
            )

    if "POS_EVER_YN" in spatial_summary_sdf.columns:
        spatial_summary_sdf["POS_EVER_YN"] = (
            spatial_summary_sdf["POS_EVER_YN"]
            .fillna("No")
            .astype(str)
        )
    else:
        spatial_summary_sdf["POS_EVER_YN"] = "No"

    for field in ["AS_OF_DATE", "CUR_FY_START", "PRIOR_FY_START", "PRIOR_FY_ASOF"]:
        if field in spatial_summary_sdf.columns:
            spatial_summary_sdf[field] = (
                spatial_summary_sdf[field]
                .fillna("")
                .astype(str)
            )

    spatial_summary_sdf = spatial_summary_sdf.drop(columns=["WMU_JOIN_KEY"])

    logger.info(
        f"Built spatial WMU summary with "
        f"{len(spatial_summary_sdf):,} polygon features."
    )

    return spatial_summary_sdf

# -----------------------------------------------------------------------------
# ArcGIS Online hosted feature layer publishing/updating
# -----------------------------------------------------------------------------

def connect_to_agol() -> GIS:
    """
    Connect to ArcGIS Online / Portal.

    Priority:
    1. AGOL_PROFILE
    2. AGOL_USERNAME / AGOL_PASSWORD
    3. GIS("home")
    """

    if AGOL_PROFILE:
        logger.info(f"Connecting to ArcGIS using saved profile: {AGOL_PROFILE}")
        return GIS(profile=AGOL_PROFILE)

    if AGOL_USERNAME and AGOL_PASSWORD:
        logger.info("Connecting to ArcGIS using configured username/password.")

        return GIS(
            AGOL_URL,
            AGOL_USERNAME,
            AGOL_PASSWORD,
        )

    logger.info("Connecting to ArcGIS using GIS('home').")
    return GIS("home")


def make_service_name(title: str) -> str:
    """
    Create a service-name-safe version of the title.

    The service name becomes part of the hosted feature service URL.
    """

    cleaned = (
        title.strip()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
        .replace("\\", "_")
    )

    cleaned = "".join(
        char for char in cleaned
        if char.isalnum() or char == "_"
    )

    return cleaned[:90]


def find_existing_summary_item(
    gis: GIS,
    title: str = WMU_SUMMARY_LAYER_TITLE,
    item_id: Optional[str] = WMU_SUMMARY_ITEM_ID,
):
    """
    Find the existing hosted feature layer item.

    Priority:
    1. Explicit WMU_SUMMARY_ITEM_ID
    2. Exact title match
    3. Service-name match, useful after partial failed publishes

    The service-name match matters because a failed first publish can leave
    behind an empty hosted feature service whose title is the service name
    rather than the human-readable layer title.
    """

    if item_id:
        item = gis.content.get(item_id)

        if item is None:
            raise ValueError(
                f"WMU_SUMMARY_ITEM_ID was provided but not found: {item_id}"
            )

        logger.info(f"Found existing summary layer by item ID: {item.id}")
        return item

    owner = gis.users.me.username
    service_name = make_service_name(title)

    search_queries = [
        f'title:"{title}" AND owner:{owner}',
        f'title:"{service_name}" AND owner:{owner}',
        f'{service_name} AND owner:{owner}',
    ]

    candidates = []

    for query in search_queries:
        logger.info(f"Searching for existing hosted layer with query: {query}")

        results = gis.content.search(
            query=query,
            item_type="Feature Layer",
            max_items=50,
        )

        for item in results:
            if item.id not in {candidate.id for candidate in candidates}:
                candidates.append(item)

    exact_matches = []

    for item in candidates:
        item_title = item.title or ""
        item_url = item.url or ""

        title_matches = item_title in {title, service_name}
        url_matches = f"/{service_name}/FeatureServer" in item_url

        if title_matches or url_matches:
            exact_matches.append(item)

    if len(exact_matches) > 1:
        details = [
            {
                "title": item.title,
                "id": item.id,
                "url": item.url,
            }
            for item in exact_matches
        ]

        raise ValueError(
            "Found multiple possible hosted feature layers for this summary. "
            "Set WMU_SUMMARY_ITEM_ID in your .env to remove ambiguity. "
            f"Matches: {details}"
        )

    if len(exact_matches) == 1:
        item = exact_matches[0]

        logger.info(
            "Found existing summary layer/service: "
            f"title={item.title}; id={item.id}; url={item.url}"
        )

        return item

    logger.info("No existing summary layer found.")
    return None


def build_wmu_summary_attribute_fields() -> list[dict]:
    """
    Build the explicitly approved AGOL attribute schema for the WMU summary.

    Field names and ArcGIS types are defined in code rather than inferred from
    pandas dtypes so schema changes are deliberate and reviewable.
    """

    fields = []

    for field_name in WMU_SUMMARY_STRING_FIELDS:
        length = 255

        if field_name == "POS_EVER_YN":
            length = 3
        elif field_name in {
            "AS_OF_DATE",
            "CUR_FY_START",
            "PRIOR_FY_START",
            "PRIOR_FY_ASOF",
        }:
            length = 10

        fields.append(
            {
                "name": field_name,
                "alias": field_name,
                "type": "esriFieldTypeString",
                "sqlType": "sqlTypeNVarchar",
                "length": length,
                "nullable": True,
                "editable": True,
            }
        )

    for field_name in WMU_SUMMARY_INTEGER_FIELDS:
        fields.append(
            {
                "name": field_name,
                "alias": field_name,
                "type": "esriFieldTypeInteger",
                "sqlType": "sqlTypeInteger",
                "nullable": True,
                "editable": True,
            }
        )

    for field_name in WMU_SUMMARY_DOUBLE_FIELDS:
        fields.append(
            {
                "name": field_name,
                "alias": field_name,
                "type": "esriFieldTypeDouble",
                "sqlType": "sqlTypeFloat",
                "nullable": True,
                "editable": True,
            }
        )

    return fields


def validate_wmu_summary_dataframe_fields(
    spatial_summary_sdf: pd.DataFrame,
) -> None:
    """
    Confirm that the spatial summary contains exactly the approved WMU summary
    attribute fields, plus its geometry column.
    """

    geometry_field = spatial_summary_sdf.spatial.name

    incoming_fields = {
        column
        for column in spatial_summary_sdf.columns
        if column != geometry_field
    }

    expected_fields = set(WMU_SUMMARY_EXPECTED_FIELDS)

    unexpected_fields = sorted(incoming_fields - expected_fields)
    missing_fields = sorted(expected_fields - incoming_fields)

    if unexpected_fields:
        raise ValueError(
            "WMU summary contains unexpected fields that are not approved "
            f"for publication to AGOL: {unexpected_fields}"
        )

    if missing_fields:
        raise ValueError(
            "WMU summary is missing fields required by the published "
            f"AGOL schema: {missing_fields}"
        )

    logger.info(
        f"WMU summary schema validation passed with "
        f"{len(expected_fields):,} approved attribute fields."
    )


def build_layer_fields_from_sdf(
    spatial_summary_sdf: pd.DataFrame,
) -> list[dict]:
    """
    Build the complete hosted-layer schema using the explicitly approved fields.
    """

    validate_wmu_summary_dataframe_fields(spatial_summary_sdf)

    fields = [
        {
            "name": "OBJECTID",
            "type": "esriFieldTypeOID",
            "alias": "OBJECTID",
            "sqlType": "sqlTypeInteger",
            "nullable": False,
            "editable": False,
        }
    ]

    fields.extend(build_wmu_summary_attribute_fields())

    return fields


def get_spatial_reference_from_sdf(
    spatial_summary_sdf: pd.DataFrame,
) -> dict:
    """
    Get spatial reference from the spatial dataframe.

    Falls back to BC Albers if the spatial accessor does not expose it cleanly.
    """

    try:
        sr = spatial_summary_sdf.spatial.sr

        if sr:
            if isinstance(sr, dict):
                return sr

            if hasattr(sr, "as_dict"):
                sr_dict = sr.as_dict

                if isinstance(sr_dict, dict):
                    return sr_dict
    except Exception:
        pass

    return {"wkid": 3005, "latestWkid": 3005}


def build_wmu_summary_layer_definition(
    spatial_summary_sdf: pd.DataFrame,
    layer_name: str = WMU_SUMMARY_LAYER_TITLE,
) -> dict:
    """
    Build a hosted polygon feature-layer definition.
    """

    return {
        "type": "Feature Layer",
        "name": make_service_name(layer_name),
        "displayField": WMU_FOREIGN_KEY_FIELD,
        "geometryType": "esriGeometryPolygon",
        "spatialReference": get_spatial_reference_from_sdf(spatial_summary_sdf),
        "fields": build_layer_fields_from_sdf(spatial_summary_sdf),
        "objectIdField": "OBJECTID",
        "uniqueField": {
            "name": "OBJECTID",
            "isSystemMaintained": True,
        },
        "indexes": [
            {
                "name": "PK_IDX",
                "fields": "OBJECTID",
                "isAscending": True,
                "isUnique": True,
                "description": "clustered, unique, primary key",
            },
            {
                "name": "WMU_IDX",
                "fields": WMU_FOREIGN_KEY_FIELD,
                "isAscending": True,
                "isUnique": False,
                "description": "WMU lookup index",
            },
        ],
        "capabilities": "Query,Create,Update,Delete,Editing",
        "hasAttachments": False,
    }


def chunk_list(values: list, chunk_size: int) -> Iterable[list]:
    for start in range(0, len(values), chunk_size):
        yield values[start:start + chunk_size]


def prepare_features_for_editing(
    spatial_summary_sdf: pd.DataFrame,
):
    """
    Convert the Spatially Enabled DataFrame to ArcGIS features and replace
    pandas missing values with None so ArcGIS receives valid JSON nulls.
    """

    feature_set = spatial_summary_sdf.spatial.to_featureset()
    features = feature_set.features

    for feature in features:
        for key, value in list(feature.attributes.items()):
            if pd.isna(value):
                feature.attributes[key] = None

    return features


def add_features_to_layer(
    layer,
    spatial_summary_sdf: pd.DataFrame,
    batch_size: int = 100,
) -> None:
    """
    Add dataframe features to a hosted feature layer in batches.
    """

    features = prepare_features_for_editing(spatial_summary_sdf)

    logger.info(f"Adding {len(features):,} features.")

    for feature_chunk in chunk_list(features, batch_size):
        add_result = layer.edit_features(adds=feature_chunk)
        add_results = add_result.get("addResults", [])

        failed = [
            result for result in add_results
            if not result.get("success", False)
        ]

        if failed:
            raise RuntimeError(f"One or more feature adds failed: {failed[:5]}")


def create_summary_feature_layer(
    gis: GIS,
    spatial_summary_sdf: pd.DataFrame,
    title: str = WMU_SUMMARY_LAYER_TITLE,
):
    """
    Create an empty hosted feature service, define a polygon layer directly,
    and then add the WMU summary features.

    This intentionally avoids spatial.to_featurelayer(), because that path can
    fall back to shapefile export when only pyshp is installed, which causes the
    10-character field-name limit.
    """

    logger.info(f"Creating new hosted feature service directly: {title}")

    service_name = make_service_name(title)

    if not gis.content.is_service_name_available(service_name, "featureService"):
        raise ValueError(
            f"The service name '{service_name}' is unavailable, but no matching "
            "owned item was found by the script. This usually means the service "
            "exists under another owner, or the item title/URL differs from the "
            "expected pattern. Find the existing item in AGOL and set "
            "WMU_SUMMARY_ITEM_ID in the runtime environment, or choose a different "
            "WMU_SUMMARY_LAYER_TITLE."
        )

    service_item = gis.content.create_service(
        name=service_name,
        service_type="featureService",
        folder=WMU_SUMMARY_FOLDER,
    )

    logger.info(f"Created empty hosted feature service item: {service_item.id}")

    try:
        service_item.update(
            item_properties={
                "title": title,
                "tags": WMU_SUMMARY_LAYER_TAGS,
                "description": (
                    "Materialized CWD sample summary by Wildlife Management Unit. "
                    "Geometry comes from the authoritative WMU polygon layer; "
                    "attributes are generated from the public CWD reporting table."
                ),
            }
        )
    except Exception as exc:
        logger.warning(f"Could not update item metadata: {exc}")

    flc = FeatureLayerCollection.fromitem(service_item)

    layer_definition = build_wmu_summary_layer_definition(
        spatial_summary_sdf=spatial_summary_sdf,
        layer_name=title,
    )

    add_definition_result = flc.manager.add_to_definition(
        {
            "layers": [layer_definition],
        }
    )

    logger.info(f"Add layer definition result: {add_definition_result}")

    service_item = gis.content.get(service_item.id)

    if not service_item.layers:
        raise RuntimeError(
            "Hosted feature service was created, but no layers were found: "
            f"{service_item.id}"
        )

    layer = service_item.layers[0]

    add_features_to_layer(
        layer=layer,
        spatial_summary_sdf=spatial_summary_sdf,
        batch_size=100,
    )

    final_count = layer.query(return_count_only=True)

    logger.info(
        f"Created hosted feature layer item: {service_item.id}; "
        f"feature count: {final_count:,}"
    )

    return service_item


def initialize_empty_summary_service(
    gis: GIS,
    item,
    spatial_summary_sdf: pd.DataFrame,
):
    """
    If a previous failed run created an empty hosted feature service with no
    layers, add the layer definition and load the features.
    """

    logger.warning(
        f"Existing item '{item.title}' was found, but it has no layers. "
        "Assuming this is a partially created service from a failed publish."
    )

    flc = FeatureLayerCollection.fromitem(item)

    layer_definition = build_wmu_summary_layer_definition(
        spatial_summary_sdf=spatial_summary_sdf,
        layer_name=item.title or WMU_SUMMARY_LAYER_TITLE,
    )

    add_definition_result = flc.manager.add_to_definition(
        {
            "layers": [layer_definition],
        }
    )

    logger.info(f"Add layer definition result: {add_definition_result}")

    item = gis.content.get(item.id)

    if not item.layers:
        raise RuntimeError(
            f"Layer definition was added, but the item still has no layers: {item.id}"
        )

    layer = item.layers[0]

    add_features_to_layer(
        layer=layer,
        spatial_summary_sdf=spatial_summary_sdf,
        batch_size=100,
    )

    final_count = layer.query(return_count_only=True)
    logger.info(f"Initialized hosted feature layer count: {final_count:,}")

    return item


def get_layer_attribute_fields(layer) -> set[str]:
    """
    Return non-system attribute fields from a hosted feature layer.
    """

    system_field_types = {
        "esriFieldTypeOID",
        "esriFieldTypeGeometry",
        "esriFieldTypeGlobalID",
    }

    system_field_names = {
        "OBJECTID",
        "ObjectId",
        "FID",
        "Shape__Area",
        "Shape__Length",
        "GlobalID",
    }

    return {
        field["name"]
        for field in layer.properties.fields
        if field.get("type") not in system_field_types
        and field.get("name") not in system_field_names
    }


def ensure_existing_layer_schema(
    spatial_summary_sdf: pd.DataFrame,
    layer,
) -> None:
    """
    Ensure the existing AGOL layer contains every explicitly approved WMU
    summary field.

    Missing approved fields are added automatically. Unexpected dataframe
    fields are rejected so schema changes remain explicit in this script.
    """

    validate_wmu_summary_dataframe_fields(spatial_summary_sdf)

    existing_fields = get_layer_attribute_fields(layer)
    expected_field_definitions = build_wmu_summary_attribute_fields()

    missing_field_definitions = [
        field_def
        for field_def in expected_field_definitions
        if field_def["name"] not in existing_fields
    ]

    if not missing_field_definitions:
        logger.info(
            "Existing AGOL layer already contains all required WMU summary fields."
        )
        return

    missing_field_names = [
        field_def["name"]
        for field_def in missing_field_definitions
    ]

    logger.info(
        "Adding missing approved fields to existing AGOL layer: "
        + ", ".join(missing_field_names)
    )

    add_result = layer.manager.add_to_definition(
        {
            "fields": missing_field_definitions,
        }
    )

    if not add_result or not add_result.get("success", False):
        raise RuntimeError(
            "Failed to add required fields to existing AGOL layer. "
            f"Fields: {missing_field_names}. "
            f"ArcGIS response: {add_result}"
        )

    # Refresh layer properties and verify that every requested field now exists.
    layer.manager.refresh()
    updated_fields = get_layer_attribute_fields(layer)
    still_missing = sorted(set(missing_field_names) - updated_fields)

    if still_missing:
        raise RuntimeError(
            "ArcGIS reported success adding fields, but these fields are still "
            f"missing from the layer schema: {still_missing}"
        )

    logger.info(
        f"Successfully added {len(missing_field_names):,} approved fields "
        "to the existing AGOL layer."
    )


def replace_features_in_existing_summary_layer(
    item,
    spatial_summary_sdf: pd.DataFrame,
    layer_index: int = 0,
    batch_size: int = 100,
):
    """
    Replace all features in an existing hosted feature layer while preserving
    the AGOL item and layer URL.
    """

    if not item.layers:
        raise ValueError(f"Item has no feature layers: {item.id}")

    layer = item.layers[layer_index]

    ensure_existing_layer_schema(
        spatial_summary_sdf=spatial_summary_sdf,
        layer=layer,
    )

    existing_count = layer.query(return_count_only=True)
    logger.info(f"Existing hosted layer feature count: {existing_count:,}")

    logger.info("Deleting existing features from hosted summary layer.")
    delete_result = layer.delete_features(where="1=1")
    logger.info(f"Delete result: {delete_result}")

    add_features_to_layer(
        layer=layer,
        spatial_summary_sdf=spatial_summary_sdf,
        batch_size=batch_size,
    )

    final_count = layer.query(return_count_only=True)
    logger.info(f"Final hosted layer feature count: {final_count:,}")

    return item


def share_item_to_group(
    gis: GIS,
    item,
    group_id: Optional[str] = WMU_SUMMARY_GROUP_ID,
):
    """
    Ensure the hosted feature layer item is shared to the configured AGOL group.

    This function is idempotent:
    - if the item is already shared to the group, do nothing;
    - otherwise attempt to share it;
    - verify the actual sharing state afterward.
    """

    if not group_id:
        logger.info(
            "No WMU_SUMMARY_GROUP_ID set. Skipping group sharing."
        )
        return item

    group = gis.groups.get(group_id)

    if group is None:
        raise ValueError(
            f"WMU summary group was not found: {group_id}"
        )

    logger.info(
        f"Checking sharing for item '{item.title}' "
        f"against group '{group.title}' ({group.id})."
    )

    # ------------------------------------------------------------------
    # Check whether it is already shared
    # ------------------------------------------------------------------

    existing_groups = item.sharing.groups.list()

    existing_group_ids = {
        shared_group.id
        for shared_group in existing_groups
    }

    if group_id in existing_group_ids:
        logger.info(
            f"Item is already shared to group: {group.title}"
        )
        return item

    # ------------------------------------------------------------------
    # Attempt sharing
    # ------------------------------------------------------------------

    logger.info(
        f"Sharing item '{item.title}' to group: "
        f"{group.title} ({group.id})"
    )

    share_result = item.sharing.groups.add(
        group=group
    )

    logger.info(
        f"ArcGIS sharing operation returned: {share_result}"
    )

    # ------------------------------------------------------------------
    # Verify actual sharing state
    # ------------------------------------------------------------------

    existing_groups = item.sharing.groups.list()

    existing_group_ids = {
        shared_group.id
        for shared_group in existing_groups
    }

    if group_id in existing_group_ids:
        logger.info(
            f"Confirmed item is shared to group: {group.title}"
        )
        return item

    # ------------------------------------------------------------------
    # Sharing genuinely failed
    # ------------------------------------------------------------------

    raise RuntimeError(
        f"Could not share item '{item.id}' to group "
        f"'{group.title}' ({group_id}). "
        f"ArcGIS returned: {share_result}. "
        f"Current shared groups: {sorted(existing_group_ids)}"
    )


def create_or_update_wmu_summary_layer(
    spatial_summary_sdf: pd.DataFrame,
):
    """
    Create the hosted feature layer if missing.

    If a previous run created an empty service but failed before adding a layer,
    initialize that empty service.

    If the layer already exists, replace its feature contents while preserving
    the item, URL, and schema.
    """

    gis = connect_to_agol()
    existing_item = find_existing_summary_item(gis)

    if existing_item is None:
        item = create_summary_feature_layer(
            gis=gis,
            spatial_summary_sdf=spatial_summary_sdf,
        )

    elif not existing_item.layers:
        item = initialize_empty_summary_service(
            gis=gis,
            item=existing_item,
            spatial_summary_sdf=spatial_summary_sdf,
        )

    else:
        item = replace_features_in_existing_summary_layer(
            item=existing_item,
            spatial_summary_sdf=spatial_summary_sdf,
        )

    item = share_item_to_group(
        gis=gis,
        item=item,
    )

    return item


# -----------------------------------------------------------------------------
# Main pipeline
# -----------------------------------------------------------------------------

def main() -> None:
    # One authenticated S3 client is reused for the input and every Excel output.
    s3_client = get_s3_client()

    # ------------------------------------------------------------------
    # Input
    # ------------------------------------------------------------------

    df = read_excel_from_s3(
        s3_client=s3_client,
        bucket=S3_INPUT_BUCKET,
        key=S3_INPUT_KEY,
        sheet_name=SHEET_NAME,
    )

    df = clean_primary_key(
        df=df,
        primary_key=PRIMARY_KEY,
    )

    # ------------------------------------------------------------------
    # WMU validation / QA
    # ------------------------------------------------------------------

    wmu_values = get_valid_wmu_values()

    df, wmu_issues_df = assign_wmu_foreign_key(
        df,
        valid_wmu_values=wmu_values,
    )

    save_dataframe_to_s3_excel(
        s3_client=s3_client,
        df=wmu_issues_df,
        bucket=S3_OUTPUT_BUCKET,
        key=qa_output_key("wmu_failures.xlsx"),
        sheet_name="WMU_Failures",
    )

    # ------------------------------------------------------------------
    # Species / status QA
    # ------------------------------------------------------------------

    df, species_summary_df = validate_species_field(
        df=df,
        species_field="SPECIES",
    )

    save_dataframe_to_s3_excel(
        s3_client=s3_client,
        df=species_summary_df,
        bucket=S3_OUTPUT_BUCKET,
        key=qa_output_key("species_summary.xlsx"),
        sheet_name="Species_Summary",
    )

    df, status_summary_df = simplify_cwd_test_status(
        df=df,
    )

    save_dataframe_to_s3_excel(
        s3_client=s3_client,
        df=status_summary_df,
        bucket=S3_OUTPUT_BUCKET,
        key=qa_output_key("status_summary.xlsx"),
        sheet_name="Status_Summary",
    )

    # ------------------------------------------------------------------
    # Date QA
    # ------------------------------------------------------------------

    df, date_summary_df, date_completeness_df = standardize_date_fields(
        df=df,
        date_fields=DATE_FIELDS,
    )

    save_dataframe_to_s3_excel(
        s3_client=s3_client,
        df=date_summary_df,
        bucket=S3_OUTPUT_BUCKET,
        key=qa_output_key("date_summary.xlsx"),
        sheet_name="Date_Summary",
    )

    save_dataframe_to_s3_excel(
        s3_client=s3_client,
        df=date_completeness_df,
        bucket=S3_OUTPUT_BUCKET,
        key=qa_output_key("date_completeness.xlsx"),
        sheet_name="Date_Completeness",
    )

    # ------------------------------------------------------------------
    # Public reporting transformations
    # ------------------------------------------------------------------

    df = calculate_test_turnaround_days(df)

    df = normalize_sample_source(
        df,
        "SUBMITTER_SOURCE_CATEGORY",
    )

    df = normalize_species(
        df,
        "SPECIES",
    )

    publication_df = export_publication_excel(
        df=df,
        s3_client=s3_client,
        bucket=S3_OUTPUT_BUCKET,
        key=PUBLICATION_OUTPUT_KEY,
    )

    # ------------------------------------------------------------------
    # WMU summary
    # ------------------------------------------------------------------

    wmu_summary_df = make_wmu_summary(publication_df)

    save_dataframe_to_s3_excel(
        s3_client=s3_client,
        df=wmu_summary_df,
        bucket=S3_OUTPUT_BUCKET,
        key=WMU_SUMMARY_OUTPUT_KEY,
        sheet_name="WMU_Summary",
    )

    # ------------------------------------------------------------------
    # ArcGIS Online publication
    # ------------------------------------------------------------------

    wmu_summary_sdf = build_wmu_summary_spatial_dataframe(
        wmu_summary_df
    )

    summary_item = create_or_update_wmu_summary_layer(
        wmu_summary_sdf
    )

    logger.info(
        "WMU summary hosted feature layer ready. "
        f"Title: {summary_item.title}; Item ID: {summary_item.id}"
    )

    logger.info(
        "Pipeline complete. No Excel outputs were written to the local "
        "GitHub Actions workspace."
    )


# %%
if __name__ == "__main__":
    main()

# %%