# %%
from io import BytesIO
import logging
import os
from zoneinfo import ZoneInfo

import boto3
import pandas as pd
import plotly.graph_objects as go
from botocore.config import Config as BotoConfig


# =============================================================================
# Configuration
# =============================================================================


def require_env(name: str) -> str:
    """Return a required environment variable or raise a clear error."""
    value = os.getenv(name)

    if value is None or not value.strip():
        raise ValueError(f"Missing required environment variable: {name}")

    return value.strip()


def env_or_default(name: str, default: str) -> str:
    """Return a stripped environment variable or a default."""
    value = os.getenv(name)

    if value is None or not value.strip():
        return default

    return value.strip()


def parse_sheet_name(value: str):
    """Allow SHEET_NAME to be either an integer index or a worksheet name."""
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

# Input produced by the preceding CWD public-reporting pipeline.
S3_INPUT_BUCKET = env_or_default(
    "S3_INPUT_BUCKET",
    "whcwdp",
)

S3_INPUT_KEY = env_or_default(
    "S3_INPUT_KEY",
    "CWD_Reporting_Staging/cwd_public_reporting.xlsx",
)

# Output location. The workflow example uses whcwdpbcbox because that is where
# the existing public HTML files are stored.
S3_OUTPUT_BUCKET = env_or_default(
    "S3_OUTPUT_BUCKET",
    "whcwdpbcbox",
)

S3_HTML_OUTPUT_KEY = env_or_default(
    "S3_HTML_OUTPUT_KEY",
    "cwd_samples_by_month.html",
)

S3_EXCEL_OUTPUT_KEY = env_or_default(
    "S3_EXCEL_OUTPUT_KEY",
    "cwd_samples_by_month.xlsx",
)

SHEET_NAME = parse_sheet_name(
    env_or_default("SHEET_NAME", "0")
)


# -----------------------------------------------------------------------------
# Reporting window
# -----------------------------------------------------------------------------

DATE_FIELD = env_or_default(
    "DATE_FIELD",
    "SAMPLED_DATE",
)

CHART_START_DATE = pd.Timestamp(
    env_or_default(
        "CHART_START_DATE",
        "2023-04-01",
    )
)

REPORTING_TIMEZONE = env_or_default(
    "REPORTING_TIMEZONE",
    "America/Vancouver",
)

# GitHub-hosted runners use UTC. Calculate the reporting date in BC local time
# so a run near midnight UTC does not unexpectedly cross a reporting month.
TODAY = (
    pd.Timestamp.now(tz=ZoneInfo(REPORTING_TIMEZONE))
    .normalize()
    .tz_localize(None)
)

# Use only fully completed calendar months.
CHART_END_DATE = (
    TODAY.to_period("M")
    .to_timestamp()
    - pd.Timedelta(days=1)
)

# First day of the current month.
# This gives the final completed month a little more room on the x-axis.
CHART_X_END = CHART_END_DATE + pd.Timedelta(days=1)


# -----------------------------------------------------------------------------
# Plotly
# -----------------------------------------------------------------------------

INCLUDE_PLOTLYJS_RAW = env_or_default(
    "INCLUDE_PLOTLYJS",
    "cdn",
)

if INCLUDE_PLOTLYJS_RAW.lower() == "true":
    INCLUDE_PLOTLYJS = True
elif INCLUDE_PLOTLYJS_RAW.lower() == "false":
    INCLUDE_PLOTLYJS = False
else:
    INCLUDE_PLOTLYJS = INCLUDE_PLOTLYJS_RAW


# =============================================================================
# Dashboard visual theme
# =============================================================================

DASHBOARD_COLORS = {
    "navy": "#213C6E",
    "blue": "#149ECE",
    "orange": "#FF7043",
    "gray": "#9E9E9E",
    "dark_text": "#2F2F2F",
    "muted_text": "#666666",
    "grid": "#D9DDE2",
    "border": "#E5E7EA",
    "panel": "#F1F4F6",
    "white": "#FFFFFF",
}

FONT_FAMILY = "Avenir Next, Avenir, Arial, Helvetica, sans-serif"


def apply_dashboard_theme(fig: go.Figure) -> go.Figure:
    """Apply styling consistent with the ArcGIS Dashboard."""

    fig.update_layout(
        autosize=True,

        font={
            "family": FONT_FAMILY,
            "color": DASHBOARD_COLORS["dark_text"],
            "size": 12,
        },

        plot_bgcolor=DASHBOARD_COLORS["white"],
        paper_bgcolor=DASHBOARD_COLORS["white"],

        margin={
            "l": 58,
            "r": 24,
            "t": 72,
            "b": 48,
        },

        hoverlabel={
            "bgcolor": DASHBOARD_COLORS["white"],
            "bordercolor": DASHBOARD_COLORS["border"],
            "font": {
                "family": FONT_FAMILY,
                "size": 13,
                "color": DASHBOARD_COLORS["dark_text"],
            },
        },

        showlegend=False,
    )

    fig.update_xaxes(
        showline=True,
        linecolor=DASHBOARD_COLORS["navy"],
        linewidth=1,

        showgrid=False,
        zeroline=False,

        tickfont={
            "family": FONT_FAMILY,
            "size": 11,
            "color": DASHBOARD_COLORS["dark_text"],
        },

        fixedrange=True,
    )

    fig.update_yaxes(
        showline=False,

        gridcolor=DASHBOARD_COLORS["grid"],
        gridwidth=1,

        zeroline=False,

        tickfont={
            "family": FONT_FAMILY,
            "size": 11,
            "color": DASHBOARD_COLORS["dark_text"],
        },

        title_font={
            "family": FONT_FAMILY,
            "size": 12,
            "color": DASHBOARD_COLORS["dark_text"],
        },

        fixedrange=True,
    )

    return fig


# =============================================================================
# Logging
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# =============================================================================
# Object Storage helpers and source data
# =============================================================================


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


def load_data_from_s3(
    s3_client,
    bucket: str,
    key: str,
    sheet_name=0,
) -> pd.DataFrame:
    """Read the staged public-reporting workbook directly from Object Storage."""

    logger.info(
        "Reading input workbook from s3://%s/%s",
        bucket,
        key,
    )

    response = s3_client.get_object(
        Bucket=bucket,
        Key=key,
    )

    file_bytes = response["Body"].read()

    logger.info(
        "Downloaded %s bytes from Object Storage",
        f"{len(file_bytes):,}",
    )

    df = pd.read_excel(
        BytesIO(file_bytes),
        sheet_name=sheet_name,
        engine="openpyxl",
        dtype={
            "CWD_EAR_CARD_ID": "string",
            "WMU_FOREIGN_KEY": "string",
            "SPECIES": "string",
            "CWD_TEST_STATUS_PUBLIC": "string",
            "SEX": "string",
            "SUBMITTER_SOURCE_CATEGORY": "string",
        },
    )

    logger.info(
        "Loaded %s rows and %s columns",
        f"{len(df):,}",
        f"{len(df.columns):,}",
    )

    return df


def upload_bytes_to_s3(
    s3_client,
    bucket: str,
    key: str,
    body: bytes,
    content_type: str,
) -> None:
    """Upload bytes to Object Storage and verify the resulting object."""

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type,
    )

    response = s3_client.head_object(
        Bucket=bucket,
        Key=key,
    )

    size_bytes = response.get(
        "ContentLength",
        len(body),
    )

    logger.info(
        "Uploaded successfully: s3://%s/%s (%s bytes)",
        bucket,
        key,
        f"{size_bytes:,}",
    )


# =============================================================================
# Prepare sampled-date records
# =============================================================================

def prepare_sample_data(
    df: pd.DataFrame,
    date_field: str,
) -> pd.DataFrame:
    """
    Prepare records for monthly sample-frequency reporting.

    Records must have a valid sampled date and must fall within the
    configured reporting window.
    """

    if date_field not in df.columns:
        raise KeyError(
            f"Required field '{date_field}' "
            "was not found in the input data."
        )

    working_df = df.copy()

    # Convert source values to pandas datetime.
    working_df[date_field] = pd.to_datetime(
        working_df[date_field],
        errors="coerce",
    )

    starting_count = len(working_df)

    # -------------------------------------------------------------------------
    # Remove records with no usable sampled date
    # -------------------------------------------------------------------------

    invalid_date_count = (
        working_df[date_field]
        .isna()
        .sum()
    )

    if invalid_date_count:
        logger.warning(
            "%s records have blank or invalid %s values "
            "and will not appear in the chart",
            f"{invalid_date_count:,}",
            date_field,
        )

    working_df = working_df.dropna(
        subset=[date_field]
    ).copy()

    logger.info(
        "%s of %s records contain a valid %s",
        f"{len(working_df):,}",
        f"{starting_count:,}",
        date_field,
    )

    if working_df.empty:
        raise ValueError(
            f"No valid dates were found in '{date_field}'."
        )

    # -------------------------------------------------------------------------
    # Restrict to reporting window
    # -------------------------------------------------------------------------

    before_start_count = (
        working_df[date_field] < CHART_START_DATE
    ).sum()

    after_end_count = (
        working_df[date_field] > CHART_END_DATE
    ).sum()

    if before_start_count:
        logger.info(
            "Excluding %s records dated before %s",
            f"{before_start_count:,}",
            CHART_START_DATE.strftime("%Y-%m-%d"),
        )

    if after_end_count:
        logger.info(
            "Excluding %s records dated after the last fully "
            "completed month (%s)",
            f"{after_end_count:,}",
            CHART_END_DATE.strftime("%Y-%m-%d"),
        )

    working_df = working_df[
        working_df[date_field].between(
            CHART_START_DATE,
            CHART_END_DATE,
            inclusive="both",
        )
    ].copy()

    if working_df.empty:
        raise ValueError(
            "No sample records fall within the configured "
            "reporting window."
        )

    logger.info(
        "%s samples fall within the reporting window",
        f"{len(working_df):,}",
    )

    return working_df


# =============================================================================
# Build monthly summary
# =============================================================================

def build_monthly_summary(
    df: pd.DataFrame,
    date_field: str,
) -> pd.DataFrame:
    """
    Count samples by calendar month.

    All months in the reporting window are retained.
    Months with no samples receive a count of zero.
    """

    working_df = df.copy()

    # Convert every date to the first day of its calendar month.
    working_df["MONTH"] = (
        working_df[date_field]
        .dt.to_period("M")
        .dt.to_timestamp()
    )

    # Count samples in observed months.
    monthly = (
        working_df
        .groupby("MONTH")
        .size()
        .rename("SAMPLE_COUNT")
    )

    # -------------------------------------------------------------------------
    # Build the complete configured calendar range
    # -------------------------------------------------------------------------

    all_months = pd.date_range(
        start=CHART_START_DATE,
        end=CHART_END_DATE,
        freq="MS",
    )

    monthly = (
        monthly
        .reindex(
            all_months,
            fill_value=0,
        )
        .rename_axis("MONTH")
        .reset_index()
    )

    # Friendly month label for hover text.
    monthly["MONTH_LABEL"] = (
        monthly["MONTH"]
        .dt.strftime("%B %Y")
    )

    logger.info(
        "Monthly summary covers %s through %s",
        monthly["MONTH"].min().strftime("%B %Y"),
        monthly["MONTH"].max().strftime("%B %Y"),
    )

    return monthly


# =============================================================================
# Create chart
# =============================================================================

def create_chart(
    monthly_df: pd.DataFrame,
) -> go.Figure:
    """Create the monthly CWD sample-frequency chart."""

    total_samples = int(
        monthly_df["SAMPLE_COUNT"].sum()
    )

    fig = go.Figure()

    # -------------------------------------------------------------------------
    # Monthly sample totals
    # -------------------------------------------------------------------------

    fig.add_trace(
        go.Bar(
            x=monthly_df["MONTH"],
            y=monthly_df["SAMPLE_COUNT"],

            marker={
                "color": DASHBOARD_COLORS["blue"],
                "line": {
                    "width": 0,
                },
            },

            customdata=monthly_df[
                "MONTH_LABEL"
            ],

            hovertemplate=(
                "<b>%{customdata}</b><br>"
                "Samples collected: %{y:,}"
                "<extra></extra>"
            ),
        )
    )

    # -------------------------------------------------------------------------
    # Main layout
    # -------------------------------------------------------------------------

    fig.update_layout(
        title={
            "text": (
                "<b>CWD Samples Collected by Month</b>"
                "<br>"
                "<span style='font-size:12px; color:#666666;'>"
                "Based on sampled date"
                "</span>"
            ),

            "x": 0.01,
            "xanchor": "left",

            "font": {
                "family": FONT_FAMILY,
                "size": 18,
                "color": DASHBOARD_COLORS["dark_text"],
            },
        },

        xaxis={
            "title": None,
            "tickformat": "%b<br>%Y",

            "range": [
                CHART_START_DATE,
                CHART_X_END,
            ],
        },

        yaxis={
            "title": "Samples",
            "rangemode": "tozero",
        },

        bargap=0.15,
    )

    # Apply the same styling as the turnaround-time chart.
    apply_dashboard_theme(fig)

    # -------------------------------------------------------------------------
    # Total sample count
    # -------------------------------------------------------------------------

    fig.add_annotation(
        text=f"{total_samples:,} samples collected",

        xref="paper",
        yref="paper",

        x=1,
        y=1.10,

        xanchor="right",
        yanchor="bottom",

        showarrow=False,

        font={
            "family": FONT_FAMILY,
            "size": 12,
            "color": DASHBOARD_COLORS["muted_text"],
        },
    )

    return fig


# =============================================================================
# Build outputs in memory
# =============================================================================


def build_html_bytes(
    fig: go.Figure,
) -> bytes:
    """Build responsive HTML suitable for embedding in Experience Builder."""

    chart_html = fig.to_html(
        include_plotlyjs=INCLUDE_PLOTLYJS,
        full_html=False,

        config={
            "displayModeBar": False,
            "responsive": True,
        },

        default_width="100%",
        default_height="100%",
    )

    html = f"""
<!DOCTYPE html>
<html lang="en">

<head>

    <meta charset="UTF-8">

    <meta
        name="viewport"
        content="width=device-width, initial-scale=1.0"
    >

    <style>

        * {{
            box-sizing: border-box;
        }}

        html,
        body {{
            width: 100%;
            height: 100%;
            margin: 0;
            padding: 0;
            overflow: hidden;
            background: #FFFFFF;
        }}

        body {{
            font-family:
                "Avenir Next",
                Avenir,
                Arial,
                Helvetica,
                sans-serif;
        }}

        .dashboard-card {{
            width: 100%;
            height: 100%;

            background: #FFFFFF;

            border: 1px solid #E5E7EA;

            overflow: hidden;
        }}

        .chart-container {{
            width: 100%;
            height: 100%;
            margin: 0;
            padding: 0;
            overflow: hidden;
        }}

        .plotly-graph-div {{
            width: 100% !important;
            height: 100% !important;
        }}

    </style>

</head>

<body>

    <div class="dashboard-card">

        <div class="chart-container">
            {chart_html}
        </div>

    </div>

</body>

</html>
"""

    payload = html.encode("utf-8")

    logger.info(
        "Built HTML output in memory (%s bytes)",
        f"{len(payload):,}",
    )

    return payload


def build_excel_bytes(
    monthly_df: pd.DataFrame,
) -> bytes:
    """Build the monthly chart summary workbook entirely in memory."""

    buffer = BytesIO()

    with pd.ExcelWriter(
        buffer,
        engine="openpyxl",
    ) as writer:
        monthly_df.to_excel(
            writer,
            sheet_name="Monthly_Summary",
            index=False,
        )

        metadata_df = pd.DataFrame(
            [
                {
                    "DATE_FIELD": DATE_FIELD,
                    "CHART_START_DATE": CHART_START_DATE.strftime("%Y-%m-%d"),
                    "CHART_END_DATE": CHART_END_DATE.strftime("%Y-%m-%d"),
                    "REPORTING_TIMEZONE": REPORTING_TIMEZONE,
                    "TOTAL_SAMPLES_IN_CHART": int(
                        monthly_df["SAMPLE_COUNT"].sum()
                    ),
                }
            ]
        )

        metadata_df.to_excel(
            writer,
            sheet_name="Metadata",
            index=False,
        )

    payload = buffer.getvalue()

    logger.info(
        "Built Excel output in memory (%s bytes)",
        f"{len(payload):,}",
    )

    return payload


# =============================================================================
# Main
# =============================================================================


def main() -> None:

    logger.info(
        "Starting monthly CWD sample chart generation."
    )

    logger.info(
        "Reporting window: %s through %s",
        CHART_START_DATE.strftime("%Y-%m-%d"),
        CHART_END_DATE.strftime("%Y-%m-%d"),
    )

    s3_client = get_s3_client()

    # Load the public-reporting workbook created by the preceding pipeline.
    df = load_data_from_s3(
        s3_client=s3_client,
        bucket=S3_INPUT_BUCKET,
        key=S3_INPUT_KEY,
        sheet_name=SHEET_NAME,
    )

    # Prepare valid records within the reporting window.
    sample_df = prepare_sample_data(
        df,
        DATE_FIELD,
    )

    # Aggregate to calendar month.
    monthly_df = build_monthly_summary(
        sample_df,
        DATE_FIELD,
    )

    # Create chart.
    fig = create_chart(
        monthly_df
    )

    # Build both outputs entirely in memory.
    html_bytes = build_html_bytes(
        fig
    )

    excel_bytes = build_excel_bytes(
        monthly_df
    )

    # Upload HTML.
    upload_bytes_to_s3(
        s3_client=s3_client,
        bucket=S3_OUTPUT_BUCKET,
        key=S3_HTML_OUTPUT_KEY,
        body=html_bytes,
        content_type="text/html; charset=utf-8",
    )

    # Upload Excel summary.
    upload_bytes_to_s3(
        s3_client=s3_client,
        bucket=S3_OUTPUT_BUCKET,
        key=S3_EXCEL_OUTPUT_KEY,
        body=excel_bytes,
        content_type=(
            "application/vnd.openxmlformats-officedocument."
            "spreadsheetml.sheet"
        ),
    )

    logger.info(
        "Monthly CWD sample chart generation completed successfully."
    )


# %%
if __name__ == "__main__":
    main()

# %%
