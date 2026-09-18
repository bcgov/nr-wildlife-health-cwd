"""
Connection test for the CWD GitHub Actions environment.

This script performs READ-ONLY tests against:
1. Required environment variables
2. BC Government Object Storage / S3
3. Master CWD Excel workbook in Object Storage
4. ArcGIS Online authentication
5. WMU Feature Service

It does not create, update, or delete anything.
"""

import os
import sys
from io import BytesIO

import boto3
import pandas as pd
from botocore.config import Config
from arcgis.gis import GIS
from arcgis.features import FeatureLayer


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

S3_BUCKET = "whcwdp"

S3_MASTER_OBJECT = (
    "master_dataset/"
    "cwd_master_dataset_sampling_w_survey_results.xlsx"
)

PRIMARY_KEY = "CWD_EAR_CARD_ID"

SHEET_NAME = 0

WMU_LAYER_URL = (
    "https://services6.arcgis.com/ubm4tcTYICKBpist/"
    "arcgis/rest/services/"
    "WHSE_WILDLIFE__SimplifyPolyg1/FeatureServer/0"
)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def print_header(title):
    print()
    print("=" * 70)
    print(title)
    print("=" * 70)


def get_s3_client():
    """
    Create and return an authenticated Object Storage client.

    READ-ONLY use in this test script.
    """

    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_CWD_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_CWD_SECRET_KEY"],
        config=Config(
            retries={
                "max_attempts": 5,
                "mode": "standard",
            }
        ),
    )


# ---------------------------------------------------------------------
# Test 1: Environment variables
# ---------------------------------------------------------------------

def test_environment_variables():
    """Confirm required environment variables exist."""

    print_header("1. ENVIRONMENT VARIABLES")

    required_variables = [
        "S3_ENDPOINT",
        "S3_CWD_ACCESS_KEY",
        "S3_CWD_SECRET_KEY",
        "AGO_HOST",
        "AGO_USERNAME",
        "AGO_PASSWORD",
    ]

    missing = []

    for variable in required_variables:
        value = os.getenv(variable)

        if value:
            # Never print the actual secret.
            print(f"[PASS] {variable} is set")
        else:
            print(f"[FAIL] {variable} is not set")
            missing.append(variable)

    if missing:
        raise RuntimeError(
            "Missing required environment variables: "
            + ", ".join(missing)
        )


# ---------------------------------------------------------------------
# Test 2: Object Storage connection
# ---------------------------------------------------------------------

def test_s3_connection():
    """Test access to the CWD Object Storage bucket."""

    print_header("2. OBJECT STORAGE / S3")

    s3 = get_s3_client()

    print(f"Testing bucket: {S3_BUCKET}")
    print(f"Testing object: {S3_MASTER_OBJECT}")

    response = s3.head_object(
        Bucket=S3_BUCKET,
        Key=S3_MASTER_OBJECT,
    )

    size_bytes = response.get("ContentLength", 0)

    print("[PASS] Connected to Object Storage")
    print("[PASS] Master CWD dataset exists")
    print(f"       Object size: {size_bytes:,} bytes")

    return s3


# ---------------------------------------------------------------------
# Test 3: Read master Excel workbook
# ---------------------------------------------------------------------

def test_master_excel(s3):
    """
    Download the master CWD workbook from Object Storage and confirm
    that pandas/openpyxl can actually read it.

    Nothing is written back to Object Storage.
    """

    print_header("3. MASTER CWD EXCEL WORKBOOK")

    print(
        f"Reading: s3://{S3_BUCKET}/{S3_MASTER_OBJECT}"
    )

    response = s3.get_object(
        Bucket=S3_BUCKET,
        Key=S3_MASTER_OBJECT,
    )

    file_bytes = response["Body"].read()

    print(
        f"[PASS] Downloaded master workbook "
        f"({len(file_bytes):,} bytes)"
    )

    # Read the actual workbook into pandas.
    #
    # CWD_EAR_CARD_ID is explicitly treated as text so IDs are
    # preserved exactly as expected by the downstream pipeline.
    df = pd.read_excel(
        BytesIO(file_bytes),
        sheet_name=SHEET_NAME,
        engine="openpyxl",
        converters={
            PRIMARY_KEY: str,
        },
    )

    print("[PASS] Excel workbook opened successfully")
    print(f"       Rows: {len(df):,}")
    print(f"       Columns: {len(df.columns):,}")

    # Confirm that the primary key required by the new pipeline exists.
    if PRIMARY_KEY not in df.columns:
        raise RuntimeError(
            f"Required primary key field "
            f"{PRIMARY_KEY!r} was not found in the workbook."
        )

    print(f"[PASS] Primary key field exists: {PRIMARY_KEY}")

    non_null_primary_keys = df[PRIMARY_KEY].notna().sum()

    print(
        f"       Non-null {PRIMARY_KEY} values: "
        f"{non_null_primary_keys:,}"
    )

    return df


# ---------------------------------------------------------------------
# Test 4: ArcGIS Online
# ---------------------------------------------------------------------

def test_arcgis_connection():
    """Test ArcGIS Online authentication."""

    print_header("4. ARCGIS ONLINE")

    gis = GIS(
        os.environ["AGO_HOST"],
        os.environ["AGO_USERNAME"],
        os.environ["AGO_PASSWORD"],
    )

    user = gis.users.me

    if user is None:
        raise RuntimeError(
            "ArcGIS authentication returned no user."
        )

    print("[PASS] ArcGIS authentication successful")
    print(f"       Authenticated user: {user.username}")

    return gis


# ---------------------------------------------------------------------
# Test 5: WMU Feature Service
# ---------------------------------------------------------------------

def test_wmu_layer(gis):
    """Test read access to the WMU Feature Service."""

    print_header("5. WMU FEATURE SERVICE")

    layer = FeatureLayer(
        WMU_LAYER_URL,
        gis=gis,
    )

    count = layer.query(
        where="1=1",
        return_count_only=True,
    )

    print("[PASS] WMU Feature Service is accessible")
    print(f"       Feature count: {count:,}")


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main():

    print_header("CWD GITHUB ACTIONS CONNECTION TEST")

    print(f"Python version: {sys.version.split()[0]}")
    print(f"Platform: {sys.platform}")

    failures = []

    # -------------------------------------------------------------
    # Environment variables
    # -------------------------------------------------------------

    try:
        test_environment_variables()

    except Exception as exc:
        print(f"[FAIL] Environment variables: {exc}")
        failures.append(
            ("Environment variables", str(exc))
        )

    # -------------------------------------------------------------
    # Object Storage
    # -------------------------------------------------------------

    s3 = None

    try:
        s3 = test_s3_connection()

    except Exception as exc:
        print(f"[FAIL] Object Storage: {exc}")
        failures.append(
            ("Object Storage", str(exc))
        )

    # -------------------------------------------------------------
    # Master Excel workbook
    # -------------------------------------------------------------

    if s3 is not None:

        try:
            test_master_excel(s3)

        except Exception as exc:
            print(f"[FAIL] Master Excel workbook: {exc}")
            failures.append(
                ("Master Excel workbook", str(exc))
            )

    # -------------------------------------------------------------
    # ArcGIS Online
    # -------------------------------------------------------------

    gis = None

    try:
        gis = test_arcgis_connection()

    except Exception as exc:
        print(f"[FAIL] ArcGIS Online: {exc}")
        failures.append(
            ("ArcGIS Online", str(exc))
        )

    # -------------------------------------------------------------
    # WMU Feature Service
    # -------------------------------------------------------------

    if gis is not None:

        try:
            test_wmu_layer(gis)

        except Exception as exc:
            print(f"[FAIL] WMU Feature Service: {exc}")
            failures.append(
                ("WMU Feature Service", str(exc))
            )

    # -------------------------------------------------------------
    # Result
    # -------------------------------------------------------------

    print_header("RESULT")

    if failures:

        print(f"{len(failures)} test(s) FAILED:")

        for name, error in failures:
            print(f" - {name}: {error}")

        # A non-zero exit code makes GitHub Actions report failure.
        sys.exit(1)

    print("All connection tests PASSED.")
    sys.exit(0)


if __name__ == "__main__":
    main()
