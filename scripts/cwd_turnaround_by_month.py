"""Generate the monthly CWD test turnaround-time chart in object storage.

This GitHub Actions version reads the public-reporting workbook directly from
S3-compatible object storage and uploads the finished HTML directly to object
storage. It does not create files in the checked-out repository.
"""

from __future__ import annotations

from io import BytesIO
import logging
import os
from typing import Union

import boto3
from botocore.config import Config
import pandas as pd
import plotly.graph_objects as go


# =============================================================================
# Logging
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# =============================================================================
# Configuration
# =============================================================================


def optional_env(name: str, default: str) -> str:
    """Return a stripped environment variable or its default."""

    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return value.strip()


def required_env(name: str) -> str:
    """Return a required environment variable with a clear error if absent."""

    value = os.getenv(name)
    if value is None or not value.strip():
        raise RuntimeError(f"Required environment variable is not set: {name}")
    return value.strip()


def parse_sheet_name(value: str) -> Union[int, str]:
    """Interpret a numeric sheet setting as an Excel sheet index."""

    stripped = value.strip()
    try:
        return int(stripped)
    except ValueError:
        return stripped


def parse_include_plotlyjs(value: str) -> Union[bool, str]:
    """Convert common boolean strings while retaining Plotly modes like cdn."""

    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    return value.strip()


S3_INPUT_BUCKET = optional_env("S3_INPUT_BUCKET", "whcwdp")
S3_INPUT_KEY = optional_env(
    "S3_INPUT_KEY",
    "CWD_Reporting_Staging/cwd_public_reporting.xlsx",
)
S3_OUTPUT_BUCKET = optional_env("S3_OUTPUT_BUCKET", "whcwdpbcbox")
S3_HTML_OUTPUT_KEY = optional_env(
    "S3_HTML_OUTPUT_KEY",
    "cwd_turnaround_by_month.html",
)
SHEET_NAME = parse_sheet_name(optional_env("SHEET_NAME", "0"))

DATE_FIELD = optional_env("DATE_FIELD", "SAMPLED_DATE")
TURNAROUND_FIELD = optional_env("TURNAROUND_FIELD", "DAYS_TO_TEST_STATUS")

# Standard Tukey outlier rule.
IQR_MULTIPLIER = float(optional_env("IQR_MULTIPLIER", "1.5"))
if IQR_MULTIPLIER < 0:
    raise ValueError("IQR_MULTIPLIER must be zero or greater.")

INCLUDE_PLOTLYJS = parse_include_plotlyjs(
    optional_env("INCLUDE_PLOTLYJS", "cdn")
)

CHART_START_DATE = pd.Timestamp(
    optional_env("CHART_START_DATE", "2023-04-01")
).normalize()

REPORTING_TIMEZONE = optional_env(
    "REPORTING_TIMEZONE",
    "America/Vancouver",
)


def get_chart_end_date() -> pd.Timestamp:
    """Return the final day of the last completed local calendar month."""

    try:
        local_today = (
            pd.Timestamp.now(tz=REPORTING_TIMEZONE)
            .tz_localize(None)
            .normalize()
        )
    except Exception as exc:
        raise ValueError(
            f"Invalid REPORTING_TIMEZONE: {REPORTING_TIMEZONE}"
        ) from exc

    return local_today.to_period("M").to_timestamp() - pd.Timedelta(days=1)


CHART_END_DATE = get_chart_end_date()

if CHART_START_DATE > CHART_END_DATE:
    raise ValueError(
        "CHART_START_DATE must be on or before the end of the last "
        "completed calendar month."
    )


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
# Object storage
# =============================================================================


def create_s3_client():
    """Create the S3-compatible object-storage client."""

    return boto3.client(
        "s3",
        endpoint_url=required_env("S3_ENDPOINT"),
        aws_access_key_id=required_env("S3_CWD_ACCESS_KEY"),
        aws_secret_access_key=required_env("S3_CWD_SECRET_KEY"),
        config=Config(
            retries={
                "max_attempts": 5,
                "mode": "standard",
            }
        ),
    )


def load_data(
    s3_client,
    bucket: str,
    key: str,
    sheet_name: Union[int, str],
) -> pd.DataFrame:
    """Read the source Excel workbook directly from object storage."""

    logger.info("Reading Excel file: s3://%s/%s", bucket, key)

    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"]

    try:
        workbook_bytes = body.read()
    finally:
        body.close()

    if not workbook_bytes:
        raise ValueError(f"Input object is empty: s3://{bucket}/{key}")

    logger.info("Downloaded %s bytes", f"{len(workbook_bytes):,}")

    df = pd.read_excel(
        BytesIO(workbook_bytes),
        sheet_name=sheet_name,
    )

    logger.info(
        "Loaded %s rows and %s columns",
        f"{len(df):,}",
        f"{len(df.columns):,}",
    )

    return df


def upload_html(
    s3_client,
    html: str,
    bucket: str,
    key: str,
) -> None:
    """Upload the rendered HTML directly to object storage."""

    html_bytes = html.encode("utf-8")

    logger.info(
        "Uploading HTML chart to s3://%s/%s (%s bytes)",
        bucket,
        key,
        f"{len(html_bytes):,}",
    )

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=html_bytes,
        ContentType="text/html; charset=utf-8",
    )

    logger.info("HTML upload complete.")


# =============================================================================
# Prepare turnaround-time records
# =============================================================================


def prepare_turnaround_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep records with a valid sampled date and turnaround-time value.

    Samples without a completed turnaround time are intentionally excluded.
    Negative turnaround times are treated as invalid.
    """

    required_fields = [
        DATE_FIELD,
        TURNAROUND_FIELD,
    ]

    missing_fields = [
        field for field in required_fields if field not in df.columns
    ]

    if missing_fields:
        raise KeyError(f"Required fields not found: {missing_fields}")

    working_df = df.copy()

    working_df[DATE_FIELD] = pd.to_datetime(
        working_df[DATE_FIELD],
        errors="coerce",
    )

    working_df[TURNAROUND_FIELD] = pd.to_numeric(
        working_df[TURNAROUND_FIELD],
        errors="coerce",
    )

    starting_count = len(working_df)

    working_df = working_df.dropna(
        subset=[
            DATE_FIELD,
            TURNAROUND_FIELD,
        ]
    ).copy()

    logger.info(
        "%s of %s records contain both a valid sampled date "
        "and turnaround time",
        f"{len(working_df):,}",
        f"{starting_count:,}",
    )

    negative_count = int((working_df[TURNAROUND_FIELD] < 0).sum())

    if negative_count:
        logger.warning(
            "Removing %s records with negative turnaround times",
            f"{negative_count:,}",
        )

        working_df = working_df[
            working_df[TURNAROUND_FIELD] >= 0
        ].copy()

    if working_df.empty:
        raise ValueError(
            "No records remain with valid sampled dates and turnaround times."
        )

    return working_df


# =============================================================================
# Remove outliers
# =============================================================================


def remove_outliers(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, float, float]:
    """
    Remove turnaround-time outliers using the standard 1.5 x IQR rule.

    One threshold is calculated across the entire dataset so every month uses
    the same definition of an outlier.
    """

    values = df[TURNAROUND_FIELD]

    q1 = float(values.quantile(0.25))
    q3 = float(values.quantile(0.75))
    iqr = q3 - q1

    lower_bound = q1 - (IQR_MULTIPLIER * iqr)
    upper_bound = q3 + (IQR_MULTIPLIER * iqr)

    # Turnaround time cannot logically be negative.
    lower_bound = max(0.0, lower_bound)

    outlier_mask = (values < lower_bound) | (values > upper_bound)
    outlier_count = int(outlier_mask.sum())

    logger.info(
        "Turnaround-time quartiles: Q1 = %.1f days, Q3 = %.1f days",
        q1,
        q3,
    )
    logger.info(
        "Turnaround-time outlier bounds: %.1f to %.1f days",
        lower_bound,
        upper_bound,
    )
    logger.info(
        "Removing %s turnaround-time outliers",
        f"{outlier_count:,}",
    )

    cleaned_df = df.loc[~outlier_mask].copy()

    logger.info(
        "%s records remain after outlier removal",
        f"{len(cleaned_df):,}",
    )

    if cleaned_df.empty:
        raise ValueError(
            "No turnaround-time records remain after outlier removal."
        )

    return cleaned_df, lower_bound, upper_bound


# =============================================================================
# Build monthly summary
# =============================================================================


def build_monthly_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate monthly average and median turnaround time."""

    df = df.copy()

    df["MONTH"] = (
        df[DATE_FIELD]
        .dt.to_period("M")
        .dt.to_timestamp()
    )

    monthly = (
        df.groupby("MONTH")
        .agg(
            AVERAGE_DAYS=(TURNAROUND_FIELD, "mean"),
            MEDIAN_DAYS=(TURNAROUND_FIELD, "median"),
            SAMPLE_COUNT=(TURNAROUND_FIELD, "size"),
        )
        .reset_index()
    )

    # Months with no completed results remain blank rather than becoming zero.
    all_months = pd.DataFrame(
        {
            "MONTH": pd.date_range(
                start=CHART_START_DATE,
                end=CHART_END_DATE,
                freq="MS",
            )
        }
    )

    monthly = all_months.merge(
        monthly,
        on="MONTH",
        how="left",
    )

    monthly["MONTH_LABEL"] = monthly["MONTH"].dt.strftime("%B %Y")

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
    cleaned_df: pd.DataFrame,
) -> go.Figure:
    """Create the monthly average CWD test turnaround-time chart."""

    overall_average = cleaned_df[TURNAROUND_FIELD].mean()
    total_results = len(cleaned_df)

    fig = go.Figure()

    custom_data = monthly_df[
        [
            "MONTH_LABEL",
            "MEDIAN_DAYS",
            "SAMPLE_COUNT",
        ]
    ].to_numpy()

    fig.add_trace(
        go.Scatter(
            x=monthly_df["MONTH"],
            y=monthly_df["AVERAGE_DAYS"],
            mode="lines+markers",
            line={
                "color": DASHBOARD_COLORS["blue"],
                "width": 3,
            },
            marker={
                "color": DASHBOARD_COLORS["blue"],
                "size": 7,
                "line": {
                    "color": DASHBOARD_COLORS["white"],
                    "width": 1,
                },
            },
            customdata=custom_data,
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "Average return time: %{y:.1f} days<br>"
                "Median return time: %{customdata[1]:.1f} days<br>"
                "Completed results: %{customdata[2]:,.0f}"
                "<extra></extra>"
            ),
            connectgaps=False,
        )
    )

    fig.update_layout(
        title={
            "text": (
                "<b>Average CWD Test Return Time</b>"
                "<br>"
                "<span style='font-size:12px; color:#666666;'>"
                "Monthly average based on sampled date"
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
                CHART_END_DATE,
            ],
        },
        yaxis={
            "title": "Average days",
            "rangemode": "tozero",
        },
    )

    apply_dashboard_theme(fig)

    fig.add_hline(
        y=overall_average,
        line={
            "color": DASHBOARD_COLORS["navy"],
            "width": 2,
            "dash": "dash",
        },
    )

    fig.add_annotation(
        text=f"Overall average: {overall_average:.1f} days",
        xref="paper",
        yref="y",
        x=1,
        y=overall_average,
        xanchor="right",
        yanchor="bottom",
        showarrow=False,
        bgcolor="rgba(255,255,255,0.85)",
        font={
            "family": FONT_FAMILY,
            "size": 11,
            "color": DASHBOARD_COLORS["navy"],
        },
    )

    fig.add_annotation(
        text=f"{total_results:,} completed test results",
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
# Render responsive HTML
# =============================================================================


def render_html(fig: go.Figure) -> str:
    """Render a responsive HTML page for ArcGIS Experience Builder."""

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

    return f"""<!DOCTYPE html>
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


# =============================================================================
# Main
# =============================================================================


def main() -> None:
    """Generate and upload the CWD turnaround-time chart."""

    logger.info("Starting CWD turnaround-time chart generation.")
    logger.info(
        "Reporting window: %s through %s",
        CHART_START_DATE.date(),
        CHART_END_DATE.date(),
    )

    s3_client = create_s3_client()

    df = load_data(
        s3_client,
        S3_INPUT_BUCKET,
        S3_INPUT_KEY,
        SHEET_NAME,
    )

    turnaround_df = prepare_turnaround_data(df)

    cleaned_df, lower_bound, upper_bound = remove_outliers(turnaround_df)
    logger.info(
        "Using outlier bounds of %.1f to %.1f days",
        lower_bound,
        upper_bound,
    )

    monthly_df = build_monthly_summary(cleaned_df)
    fig = create_chart(monthly_df, cleaned_df)
    html = render_html(fig)

    upload_html(
        s3_client,
        html,
        S3_OUTPUT_BUCKET,
        S3_HTML_OUTPUT_KEY,
    )

    logger.info(
        "Turnaround-time chart generation completed successfully."
    )


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("CWD turnaround-time chart generation failed.")
        raise
