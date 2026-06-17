import logging
from pathlib import Path

from pyspark.sql import DataFrame

from billups_challenge.config import OUTPUT_DIR
from billups_challenge.questions import MerchantCityAnalysis, NewMerchantAdvice

logger = logging.getLogger(__name__)

REPORT_PATH = OUTPUT_DIR / "REPORT.md"


def _df_to_markdown(df: DataFrame, limit: int = 20) -> str:
    """Render the first N rows of a DataFrame as a markdown table."""
    rows = df.limit(limit).collect()
    if not rows:
        return "_No data available._\n"

    columns = df.columns
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"

    lines = [header, separator]
    for row in rows:
        values = []
        for col in columns:
            val = row[col]
            if isinstance(val, float):
                values.append(f"{val:,.2f}")
            elif isinstance(val, int):
                values.append(f"{val:,}")
            elif val is None:
                values.append("")
            else:
                values.append(str(val))
        lines.append("| " + " | ".join(values) + " |")

    return "\n".join(lines) + "\n"


def _quality_summary_section(quality_summary: dict[str, int]) -> str:
    """Build the data quality summary table for the report."""
    total_removed = (
        quality_summary["malformed"]
        + quality_summary["invalid_timestamps"]
        + quality_summary["outlier_amounts"]
        + quality_summary["denied_transactions"]
    )
    total_input = quality_summary["clean_rows"] + total_removed

    def _pct(value: int) -> str:
        return f"{value / total_input * 100:.2f}%"

    def _row(stage: str, count: int, bold: bool = False) -> str:
        if bold:
            return f"| **{stage}** | **{count:,}** | **{_pct(count)}** |"
        return f"| {stage} | {count:,} | {_pct(count)} |"

    lines = [
        "## Data Quality Summary\n",
        "| Stage | Rows Removed | % of Input |",
        "| --- | --- | --- |",
        _row("Malformed (null in required columns)", quality_summary["malformed"]),
        _row("Invalid timestamps (outside 2000-2030)", quality_summary["invalid_timestamps"]),
        _row("Outlier amounts (<=0 or >1M)", quality_summary["outlier_amounts"]),
        _row("Denied transactions (authorized_flag != Y)", quality_summary["denied_transactions"]),
        _row("Total removed", total_removed, bold=True),
        _row("Clean rows for analysis", quality_summary["clean_rows"], bold=True),
        "",
    ]
    return "\n".join(lines)


def generate_report(
    q1: DataFrame,
    q2: DataFrame,
    q3: DataFrame,
    q4: MerchantCityAnalysis,
    q5: NewMerchantAdvice,
    quality_summary: dict[str, int] | None = None,
) -> None:
    """Write output/REPORT.md with all query results and quality metrics."""
    sections = []

    sections.append("# Billups Data Engineering Challenge - Report\n")

    if quality_summary:
        sections.append(_quality_summary_section(quality_summary))
    sections.append("## Question 1: Top 5 Merchants by Purchase Amount (per Month, per City)\n")
    sections.append(_df_to_markdown(q1, limit=30))

    sections.append("\n## Question 2: Average Sale Amount per Merchant per State\n")
    sections.append("Ordered by largest average amount first.\n")
    sections.append(_df_to_markdown(q2, limit=20))

    sections.append("\n## Question 3: Top 3 Hours with Largest Sales per Category\n")
    sections.append(_df_to_markdown(q3, limit=30))

    sections.append("\n## Question 4: Cities Where Popular Merchants Are Located\n")
    sections.append("### Most Popular Merchants (by transaction count) and Their Primary City\n")
    sections.append(_df_to_markdown(q4.popular_merchants, limit=20))
    sections.append("\n### Category Correlation by City\n")
    sections.append("Top categories per city (by number of transactions):\n")
    sections.append(_df_to_markdown(q4.category_correlation, limit=20))

    sections.append("\n## Question 5: Business Advice for a New Merchant\n")

    sections.append("### a) Recommended Cities\n")
    sections.append("Top 10 cities by total revenue:\n")
    sections.append(_df_to_markdown(q5.cities))

    sections.append("\n### b) Recommended Categories\n")
    sections.append("Top 10 categories by total revenue:\n")
    sections.append(_df_to_markdown(q5.categories))

    sections.append("\n### c) Monthly Patterns\n")
    sections.append("Revenue by month (look for seasonal spikes):\n")
    sections.append(_df_to_markdown(q5.monthly_patterns))

    sections.append("\n### d) Hourly Patterns\n")
    sections.append("Revenue by hour of day (useful for deciding open/close times):\n")
    sections.append(_df_to_markdown(q5.hourly_patterns))

    sections.append("\n### e) Installment Analysis\n")
    sections.append(
        "Should the merchant accept installments? Analysis considering:\n"
        "- 25% gross profit margin\n"
        "- 22.9% credit default rate on installment sales\n"
        "- Defaulters pay half before defaulting\n"
        "- Cash sales (installments=0 or 1) have no default risk\n\n"
    )
    sections.append(_df_to_markdown(q5.installment_analysis))

    sections.append(
        "\n**About the default rate model:** We apply 22.9% as a flat probability per the "
        "challenge's simplistic assumptions. In practice this rate would compound monthly "
        "(`1 - (1 - 0.229)^N`), so a 12-month plan would have ~95% cumulative default risk. "
        "With the flat model: installments yield ~13.55% margin vs 25% for cash. "
        "They're only worth it if they bring enough extra volume to compensate.\n"
    )

    report_content = "\n".join(sections)

    Path(REPORT_PATH).parent.mkdir(parents=True, exist_ok=True)
    Path(REPORT_PATH).write_text(report_content, encoding="utf-8")
    logger.info("Report generated: %s", REPORT_PATH)
