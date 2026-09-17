"""
Connection test for the CWD GitHub Actions environment.

This script performs READ-ONLY tests against:
1. Required environment variables
2. BC Government Object Storage / S3
3. ArcGIS Online authentication
4. WMU Feature Service

It does not create, update, or delete anything.
"""

import os
import sys

import boto3
from botocore.config import Config
from arcgis.gis import GIS
from arcgis.features import FeatureLayer


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

S3_BUCKET = "whcwdp"

S3_TEST_OBJECT = (
    "master_dataset/"
    "cwd_master_dataset_sampling_w_survey_results.xlsx"
)

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


def test_s3_connection():
    """Test access to the CWD Object Storage bucket."""

    print_header("2. OBJECT STORAGE / S3")

    s3 = boto3.client(
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

    print(f"Testing bucket: {S3_BUCKET}")
    print(f"Testing object: {S3_TEST_OBJECT}")

    response = s3.head_object(
        Bucket=S3_BUCKET,
        Key=S3_TEST_OBJECT,
    )

    size_bytes = response.get("ContentLength", 0)

    print("[PASS] Connected to Object Storage")
    print("[PASS] Master CWD dataset is accessible")
    print(f"       Object size: {size_bytes:,} bytes")


def test_arcgis_connection():
    """Test ArcGIS Online authentication."""

    print_header("3. ARCGIS ONLINE")

    gis = GIS(
        os.environ["AGO_HOST"],
        os.environ["AGO_USERNAME"],
        os.environ["AGO_PASSWORD"],
    )

    user = gis.users.me

    if user is None:
        raise RuntimeError("ArcGIS authentication returned no user.")

    print("[PASS] ArcGIS authentication successful")
    print(f"       Authenticated user: {user.username}")

    return gis


def test_wmu_layer(gis):
    """Test access to the WMU Feature Service."""

    print_header("4. WMU FEATURE SERVICE")

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

    tests = [
        ("Environment variables", test_environment_variables),
        ("Object Storage", test_s3_connection),
    ]

    failures = []

    for name, test_function in tests:
        try:
            test_function()
        except Exception as exc:
            print(f"[FAIL] {name}: {exc}")
            failures.append((name, str(exc)))

    # ArcGIS is slightly different because we reuse the GIS connection.
    gis = None

    try:
        gis = test_arcgis_connection()
    except Exception as exc:
        print(f"[FAIL] ArcGIS Online: {exc}")
        failures.append(("ArcGIS Online", str(exc)))

    if gis is not None:
        try:
            test_wmu_layer(gis)
        except Exception as exc:
            print(f"[FAIL] WMU Feature Service: {exc}")
            failures.append(("WMU Feature Service", str(exc)))

    print_header("RESULT")

    if failures:
        print(f"{len(failures)} test(s) FAILED:")

        for name, error in failures:
            print(f" - {name}: {error}")

        # Important for GitHub Actions:
        # a non-zero exit code makes the workflow show as failed.
        sys.exit(1)

    print("All connection tests PASSED.")
    sys.exit(0)


if __name__ == "__main__":
    main()
