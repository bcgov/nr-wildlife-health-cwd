"""Generate monthly CWD turnaround-time reporting outputs in object storage.

This GitHub Actions version reads the public-reporting workbook directly from
S3-compatible object storage and uploads the finished HTML, monthly summary
workbook, and IQR outlier audit directly to object storage. It does not create
files in the checked-out repository.
"""

from __future__ import annotations

from io import BytesIO
import logging
import os
from typing import Union

import boto3
from botocore.config import Config
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
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
S3_SUMMARY_EXCEL_OUTPUT_KEY = optional_env(
    "S3_SUMMARY_EXCEL_OUTPUT_KEY",
    "cwd_turnaround_by_month_summary.xlsx",
)
S3_OUTLIER_AUDIT_OUTPUT_KEY = optional_env(
    "S3_OUTLIER_AUDIT_OUTPUT_KEY",
    "cwd_turnaround_by_month_outliers.xlsx",
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


def upload_excel(
    s3_client,
    workbook_bytes: bytes,
    bucket: str,
    key: str,
) -> None:
    """Upload an Excel workbook directly to object storage."""

    if not workbook_bytes:
        raise ValueError(f"Refusing to upload an empty workbook: {key}")

    logger.info(
        "Uploading Excel workbook to s3://%s/%s (%s bytes)",
        bucket,
        key,
        f"{len(workbook_bytes):,}",
    )

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=workbook_bytes,
        ContentType=(
            "application/vnd.openxmlformats-officedocument."
            "spreadsheetml.sheet"
        ),
    )

    logger.info("Excel upload complete: s3://%s/%s", bucket, key)


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

    if "SOURCE_EXCEL_ROW" in working_df.columns:
        raise KeyError(
            "The reserved audit field SOURCE_EXCEL_ROW already exists "
            "in the input workbook."
        )

    # Pandas row 0 corresponds to Excel row 2 because row 1 is the header.
    working_df.insert(
        0,
        "SOURCE_EXCEL_ROW",
        range(2, len(working_df) + 2),
    )

    date_values = working_df[DATE_FIELD]
    if pd.api.types.is_datetime64_any_dtype(date_values):
        working_df[DATE_FIELD] = pd.to_datetime(
            date_values,
            errors="coerce",
        )
    else:
        # Scalar conversion safely handles columns containing a mixture of
        # date-only strings, timestamps, and Excel datetime values.
        working_df[DATE_FIELD] = date_values.map(
            lambda value: pd.to_datetime(value, errors="coerce")
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


def filter_reporting_window(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only records represented by the chart's completed-month window."""

    end_exclusive = CHART_END_DATE + pd.Timedelta(days=1)
    reporting_df = df[
        (df[DATE_FIELD] >= CHART_START_DATE)
        & (df[DATE_FIELD] < end_exclusive)
    ].copy()

    logger.info(
        "%s of %s valid records fall within the reporting window",
        f"{len(reporting_df):,}",
        f"{len(df):,}",
    )

    if reporting_df.empty:
        raise ValueError(
            "No valid turnaround-time records fall within the reporting "
            "window."
        )

    return reporting_df


# =============================================================================
# Remove outliers
# =============================================================================


def remove_outliers(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, float, float]:
    """
    Remove turnaround-time outliers using the standard 1.5 x IQR rule.

    One threshold is calculated across the reporting window so every displayed
    month uses the same definition of an outlier.
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
    outliers_df = df.loc[outlier_mask].copy()

    outliers_df.insert(
        1,
        "OUTLIER_REASON",
        "Above upper IQR bound",
    )
    outliers_df.loc[
        outliers_df[TURNAROUND_FIELD] < lower_bound,
        "OUTLIER_REASON",
    ] = "Below lower IQR bound"
    outliers_df.insert(2, "IQR_LOWER_BOUND_DAYS", lower_bound)
    outliers_df.insert(3, "IQR_UPPER_BOUND_DAYS", upper_bound)
    outliers_df.insert(4, "IQR_MULTIPLIER", IQR_MULTIPLIER)

    logger.info(
        "%s records remain after outlier removal",
        f"{len(cleaned_df):,}",
    )

    if cleaned_df.empty:
        raise ValueError(
            "No turnaround-time records remain after outlier removal."
        )

    return cleaned_df, outliers_df, lower_bound, upper_bound


# =============================================================================
# Build monthly summary
# =============================================================================


def build_monthly_summary(
    cleaned_df: pd.DataFrame,
    outliers_df: pd.DataFrame,
) -> pd.DataFrame:
    """Calculate monthly chart values and supporting summary statistics."""

    cleaned_df = cleaned_df.copy()
    outliers_df = outliers_df.copy()

    cleaned_df["MONTH"] = (
        cleaned_df[DATE_FIELD]
        .dt.to_period("M")
        .dt.to_timestamp()
    )
    outliers_df["MONTH"] = (
        outliers_df[DATE_FIELD]
        .dt.to_period("M")
        .dt.to_timestamp()
    )

    monthly = (
        cleaned_df.groupby("MONTH")
        .agg(
            AVERAGE_DAYS=(TURNAROUND_FIELD, "mean"),
            MEDIAN_DAYS=(TURNAROUND_FIELD, "median"),
            MINIMUM_DAYS=(TURNAROUND_FIELD, "min"),
            MAXIMUM_DAYS=(TURNAROUND_FIELD, "max"),
            Q1_DAYS=(TURNAROUND_FIELD, lambda values: values.quantile(0.25)),
            Q3_DAYS=(TURNAROUND_FIELD, lambda values: values.quantile(0.75)),
            STANDARD_DEVIATION_DAYS=(TURNAROUND_FIELD, "std"),
            SAMPLE_COUNT=(TURNAROUND_FIELD, "size"),
        )
        .reset_index()
    )

    monthly_outliers = (
        outliers_df.groupby("MONTH")
        .size()
        .rename("OUTLIERS_REMOVED")
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
    monthly = monthly.merge(
        monthly_outliers,
        on="MONTH",
        how="left",
    )

    for count_field in ("SAMPLE_COUNT", "OUTLIERS_REMOVED"):
        monthly[count_field] = (
            monthly[count_field]
            .fillna(0)
            .astype(int)
        )

    monthly["VALID_SAMPLE_COUNT"] = (
        monthly["SAMPLE_COUNT"] + monthly["OUTLIERS_REMOVED"]
    )

    monthly["MONTH_LABEL"] = monthly["MONTH"].dt.strftime("%B %Y")

    logger.info(
        "Monthly summary covers %s through %s",
        monthly["MONTH"].min().strftime("%B %Y"),
        monthly["MONTH"].max().strftime("%B %Y"),
    )

    return monthly


# =============================================================================
# Create Excel workbooks
# =============================================================================


def style_excel_sheet(
    worksheet,
    *,
    freeze_panes: str,
    number_formats: dict[str, str],
    tab_color: str,
) -> None:
    """Apply readable, restrained formatting to an exported worksheet."""

    header_fill = PatternFill("solid", fgColor="213C6E")
    band_fill = PatternFill("solid", fgColor="F1F4F6")
    header_font = Font(
        name="Arial",
        size=10,
        bold=True,
        color="FFFFFF",
    )
    body_font = Font(name="Arial", size=10, color="2F2F2F")

    worksheet.sheet_view.showGridLines = False
    worksheet.sheet_properties.tabColor = tab_color
    worksheet.freeze_panes = freeze_panes
    worksheet.auto_filter.ref = worksheet.dimensions
    worksheet.row_dimensions[1].height = 32

    headers = {
        cell.value: cell.column
        for cell in worksheet[1]
        if cell.value is not None
    }

    for cell in worksheet[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(
            horizontal="center",
            vertical="center",
            wrap_text=True,
        )

    for row_number in range(2, worksheet.max_row + 1):
        use_band_fill = row_number % 2 == 0
        for cell in worksheet[row_number]:
            cell.font = body_font
            cell.alignment = Alignment(vertical="center")
            if use_band_fill:
                cell.fill = band_fill

    for header, number_format in number_formats.items():
        column_number = headers.get(header)
        if column_number is None:
            continue
        for row_number in range(2, worksheet.max_row + 1):
            worksheet.cell(
                row=row_number,
                column=column_number,
            ).number_format = number_format

    # Widths are based on the header and a bounded sample of data rows.
    sampled_last_row = min(worksheet.max_row, 251)
    for column_number in range(1, worksheet.max_column + 1):
        values = [
            worksheet.cell(row=row_number, column=column_number).value
            for row_number in range(1, sampled_last_row + 1)
        ]
        max_length = max(
            (len(str(value)) for value in values if value is not None),
            default=0,
        )
        width = min(max(max_length + 2, 11), 42)
        worksheet.column_dimensions[
            get_column_letter(column_number)
        ].width = width


def build_overall_summary(
    reporting_df: pd.DataFrame,
    cleaned_df: pd.DataFrame,
    outliers_df: pd.DataFrame,
    lower_bound: float,
    upper_bound: float,
) -> pd.DataFrame:
    """Create the report-level summary used by both Excel outputs."""

    outlier_percentage = len(outliers_df) / len(reporting_df)
    generated_at = pd.Timestamp.now(tz=REPORTING_TIMEZONE).strftime(
        "%Y-%m-%d %H:%M:%S %Z"
    )

    rows = [
        {
            "Metric": "Generated at",
            "Value": generated_at,
            "Notes": "Reporting timezone",
        },
        {
            "Metric": "Reporting window start",
            "Value": CHART_START_DATE,
            "Notes": "First displayed month",
        },
        {
            "Metric": "Reporting window end",
            "Value": CHART_END_DATE,
            "Notes": "Final day of the last completed month",
        },
        {
            "Metric": "IQR multiplier",
            "Value": IQR_MULTIPLIER,
            "Notes": "Tukey IQR rule",
        },
        {
            "Metric": "Lower IQR bound (days)",
            "Value": lower_bound,
            "Notes": "Values below this bound are excluded",
        },
        {
            "Metric": "Upper IQR bound (days)",
            "Value": upper_bound,
            "Notes": "Values above this bound are excluded",
        },
        {
            "Metric": "Valid samples before IQR",
            "Value": len(reporting_df),
            "Notes": "Valid date and non-negative turnaround time",
        },
        {
            "Metric": "Outliers removed",
            "Value": len(outliers_df),
            "Notes": "Samples outside the global IQR bounds",
        },
        {
            "Metric": "Outliers removed (%)",
            "Value": outlier_percentage,
            "Notes": "Share of valid samples in the reporting window",
        },
        {
            "Metric": "Samples included in chart",
            "Value": len(cleaned_df),
            "Notes": "Valid samples after IQR removal",
        },
        {
            "Metric": "Mean turnaround (days)",
            "Value": cleaned_df[TURNAROUND_FIELD].mean(),
            "Notes": "After IQR removal",
        },
        {
            "Metric": "Median turnaround (days)",
            "Value": cleaned_df[TURNAROUND_FIELD].median(),
            "Notes": "After IQR removal",
        },
        {
            "Metric": "Minimum turnaround (days)",
            "Value": cleaned_df[TURNAROUND_FIELD].min(),
            "Notes": "After IQR removal",
        },
        {
            "Metric": "Maximum turnaround (days)",
            "Value": cleaned_df[TURNAROUND_FIELD].max(),
            "Notes": "After IQR removal",
        },
    ]

    return pd.DataFrame(rows)


def style_overall_summary_sheet(worksheet) -> None:
    """Apply formats that depend on the metric name in the first column."""

    style_excel_sheet(
        worksheet,
        freeze_panes="A2",
        number_formats={},
        tab_color="149ECE",
    )

    worksheet.column_dimensions["A"].width = 30
    worksheet.column_dimensions["B"].width = 24
    worksheet.column_dimensions["C"].width = 48

    for row_number in range(2, worksheet.max_row + 1):
        metric = worksheet.cell(row=row_number, column=1).value
        value_cell = worksheet.cell(row=row_number, column=2)

        if metric in {"Reporting window start", "Reporting window end"}:
            value_cell.number_format = "yyyy-mm-dd"
        elif metric == "Outliers removed (%)":
            value_cell.number_format = "0.0%"
        elif metric in {
            "Valid samples before IQR",
            "Outliers removed",
            "Samples included in chart",
        }:
            value_cell.number_format = "#,##0"
        elif isinstance(value_cell.value, (int, float)):
            value_cell.number_format = "0.0"


def create_summary_workbook(
    monthly_df: pd.DataFrame,
    reporting_df: pd.DataFrame,
    cleaned_df: pd.DataFrame,
    outliers_df: pd.DataFrame,
    lower_bound: float,
    upper_bound: float,
) -> bytes:
    """Create the chart-data and report-statistics Excel workbook."""

    monthly_export = monthly_df[
        [
            "MONTH",
            "AVERAGE_DAYS",
            "MEDIAN_DAYS",
            "MINIMUM_DAYS",
            "MAXIMUM_DAYS",
            "Q1_DAYS",
            "Q3_DAYS",
            "STANDARD_DEVIATION_DAYS",
            "SAMPLE_COUNT",
            "VALID_SAMPLE_COUNT",
            "OUTLIERS_REMOVED",
        ]
    ].rename(
        columns={
            "MONTH": "Month",
            "AVERAGE_DAYS": "Mean days (graph value)",
            "MEDIAN_DAYS": "Median days",
            "MINIMUM_DAYS": "Minimum days",
            "MAXIMUM_DAYS": "Maximum days",
            "Q1_DAYS": "Q1 days",
            "Q3_DAYS": "Q3 days",
            "STANDARD_DEVIATION_DAYS": "Standard deviation days",
            "SAMPLE_COUNT": "Samples included",
            "VALID_SAMPLE_COUNT": "Valid samples before IQR",
            "OUTLIERS_REMOVED": "Outliers removed",
        }
    )

    overall_export = build_overall_summary(
        reporting_df,
        cleaned_df,
        outliers_df,
        lower_bound,
        upper_bound,
    )

    buffer = BytesIO()
    with pd.ExcelWriter(
        buffer,
        engine="openpyxl",
        datetime_format="yyyy-mm-dd",
    ) as writer:
        monthly_export.to_excel(
            writer,
            sheet_name="Monthly Summary",
            index=False,
        )
        overall_export.to_excel(
            writer,
            sheet_name="Overall Summary",
            index=False,
        )

        monthly_sheet = writer.sheets["Monthly Summary"]
        style_excel_sheet(
            monthly_sheet,
            freeze_panes="B2",
            number_formats={
                "Month": "mmmm yyyy",
                "Mean days (graph value)": "0.0",
                "Median days": "0.0",
                "Minimum days": "0.0",
                "Maximum days": "0.0",
                "Q1 days": "0.0",
                "Q3 days": "0.0",
                "Standard deviation days": "0.0",
                "Samples included": "#,##0",
                "Valid samples before IQR": "#,##0",
                "Outliers removed": "#,##0",
            },
            tab_color="213C6E",
        )
        style_overall_summary_sheet(writer.sheets["Overall Summary"])

    return buffer.getvalue()


def create_outlier_audit_workbook(
    reporting_df: pd.DataFrame,
    cleaned_df: pd.DataFrame,
    outliers_df: pd.DataFrame,
    lower_bound: float,
    upper_bound: float,
) -> bytes:
    """Create a row-level audit workbook for samples removed by the IQR rule."""

    audit_export = outliers_df.copy()
    audit_export.insert(
        2,
        "SAMPLE_MONTH",
        audit_export[DATE_FIELD].dt.to_period("M").dt.to_timestamp(),
    )
    audit_export = audit_export.rename(
        columns={
            "SOURCE_EXCEL_ROW": "Source Excel row",
            "OUTLIER_REASON": "Outlier reason",
            "SAMPLE_MONTH": "Sample month",
            "IQR_LOWER_BOUND_DAYS": "IQR lower bound (days)",
            "IQR_UPPER_BOUND_DAYS": "IQR upper bound (days)",
            "IQR_MULTIPLIER": "IQR multiplier",
        }
    )

    overall_export = build_overall_summary(
        reporting_df,
        cleaned_df,
        outliers_df,
        lower_bound,
        upper_bound,
    )

    audit_formats = {
        "Source Excel row": "#,##0",
        "Sample month": "mmmm yyyy",
        "IQR lower bound (days)": "0.0",
        "IQR upper bound (days)": "0.0",
        "IQR multiplier": "0.0",
        TURNAROUND_FIELD: "0.0",
    }

    for column in audit_export.columns:
        if pd.api.types.is_datetime64_any_dtype(audit_export[column]):
            audit_formats[column] = "yyyy-mm-dd"
    audit_formats["Sample month"] = "mmmm yyyy"

    buffer = BytesIO()
    with pd.ExcelWriter(
        buffer,
        engine="openpyxl",
        datetime_format="yyyy-mm-dd",
    ) as writer:
        audit_export.to_excel(
            writer,
            sheet_name="Outliers",
            index=False,
        )
        overall_export.to_excel(
            writer,
            sheet_name="Audit Summary",
            index=False,
        )

        style_excel_sheet(
            writer.sheets["Outliers"],
            freeze_panes="G2",
            number_formats=audit_formats,
            tab_color="8C1D18",
        )
        style_overall_summary_sheet(writer.sheets["Audit Summary"])

    return buffer.getvalue()


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
    """Generate and upload the CWD turnaround-time reporting outputs."""

    logger.info("Starting CWD turnaround-time reporting generation.")
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
    reporting_df = filter_reporting_window(turnaround_df)

    (
        cleaned_df,
        outliers_df,
        lower_bound,
        upper_bound,
    ) = remove_outliers(reporting_df)
    logger.info(
        "Using outlier bounds of %.1f to %.1f days",
        lower_bound,
        upper_bound,
    )

    monthly_df = build_monthly_summary(cleaned_df, outliers_df)
    fig = create_chart(monthly_df, cleaned_df)
    html = render_html(fig)
    summary_workbook = create_summary_workbook(
        monthly_df,
        reporting_df,
        cleaned_df,
        outliers_df,
        lower_bound,
        upper_bound,
    )
    outlier_audit_workbook = create_outlier_audit_workbook(
        reporting_df,
        cleaned_df,
        outliers_df,
        lower_bound,
        upper_bound,
    )

    upload_html(
        s3_client,
        html,
        S3_OUTPUT_BUCKET,
        S3_HTML_OUTPUT_KEY,
    )
    upload_excel(
        s3_client,
        summary_workbook,
        S3_OUTPUT_BUCKET,
        S3_SUMMARY_EXCEL_OUTPUT_KEY,
    )
    upload_excel(
        s3_client,
        outlier_audit_workbook,
        S3_OUTPUT_BUCKET,
        S3_OUTLIER_AUDIT_OUTPUT_KEY,
    )

    logger.info(
        "Turnaround-time reporting outputs completed successfully."
    )


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("CWD turnaround-time chart generation failed.")
        raise

