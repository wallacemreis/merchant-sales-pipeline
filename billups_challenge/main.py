import logging

from pyspark.sql import DataFrame

from billups_challenge.config import LOG_FILE, MERCHANTS_PATH, OUTPUT_DIR, OUTPUT_FORMAT, TRANSACTIONS_PATH
from billups_challenge.ingestion import (
    MERCHANTS_SPEC,
    TRANSACTIONS_SPEC,
    clean_transactions,
    deduplicate_transactions,
    filter_denied_transactions,
    filter_invalid_timestamps,
    filter_outlier_amounts,
    quarantine_malformed_rows,
    read_merchants,
    read_transactions,
    validate_schema,
)
from billups_challenge.questions import (
    question_1_top_merchants_by_city_month,
    question_2_avg_sales_by_merchant_state,
    question_3_top_hours_by_category,
    question_4_popular_merchants_cities,
    question_5_new_merchant_advice,
)
from billups_challenge.report import generate_report
from billups_challenge.session import create_spark_session

LOG_FORMAT = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def _setup_file_logging() -> None:
    from pathlib import Path

    Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logging.getLogger().addHandler(file_handler)


WRITE_STRATEGIES = {
    "csv": lambda writer, path: writer.csv(path),
    "parquet": lambda writer, path: writer.parquet(path),
}


def save_result(df: DataFrame, name: str) -> None:
    output_path = str(OUTPUT_DIR / name)
    writer = df.coalesce(1).write.mode("overwrite").option("header", "true")

    strategy = WRITE_STRATEGIES.get(OUTPUT_FORMAT)
    if not strategy:
        raise ValueError(f"Unsupported output format: {OUTPUT_FORMAT}. Supported: {list(WRITE_STRATEGIES.keys())}")

    strategy(writer, output_path)
    logger.info("Saved %s → %s (%s)", name, output_path, OUTPUT_FORMAT)


def _safe_save(outputs: list[tuple[DataFrame | None, str]]) -> None:
    for result_df, name in outputs:
        if result_df is None:
            continue
        try:
            save_result(result_df, name)
        except Exception:
            logger.exception("Failed to save %s — continuing", name)


def _run_query(name: str, fn):
    try:
        result = fn()
        logger.info("Query %s completed", name)
        return result
    except Exception:
        logger.exception("Query %s failed — continuing", name)
        return None


def run():
    _setup_file_logging()
    spark = create_spark_session()

    logger.info("Reading datasets...")
    transactions = read_transactions(spark, str(TRANSACTIONS_PATH))
    merchants = read_merchants(spark, str(MERCHANTS_PATH))

    validate_schema(transactions, TRANSACTIONS_SPEC.required_columns, TRANSACTIONS_SPEC.name)
    validate_schema(merchants, MERCHANTS_SPEC.required_columns, MERCHANTS_SPEC.name)

    logger.info("Data quality checks...")
    transactions, malformed = quarantine_malformed_rows(transactions, TRANSACTIONS_SPEC.required_columns)
    malformed_count = malformed.count()
    if malformed_count > 0:
        logger.warning("Quarantined %d malformed rows", malformed_count)
        save_result(malformed, "quarantine_malformed")

    transactions = deduplicate_transactions(transactions)

    transactions, invalid_ts = filter_invalid_timestamps(transactions)
    invalid_ts_count = invalid_ts.count()
    if invalid_ts_count > 0:
        logger.warning("Quarantined %d rows with invalid timestamps", invalid_ts_count)
        save_result(invalid_ts, "quarantine_invalid_timestamps")

    transactions, outliers = filter_outlier_amounts(transactions)
    outlier_count = outliers.count()
    if outlier_count > 0:
        logger.warning("Quarantined %d rows with outlier amounts", outlier_count)
        save_result(outliers, "quarantine_outlier_amounts")

    transactions, denied = filter_denied_transactions(transactions)
    denied_count = denied.count()
    if denied_count > 0:
        logger.info("Filtered %d denied transactions (authorized_flag != 'Y')", denied_count)
        save_result(denied, "quarantine_denied")

    df = clean_transactions(transactions, merchants)
    df.cache()

    row_count = df.count()
    if row_count == 0:
        logger.warning("No rows remaining after data quality filters — queries will produce empty results")
    else:
        logger.info("Clean dataset: %d rows ready for analysis", row_count)

    logger.info("Running analytical queries...")

    q1 = _run_query("question_1", lambda: question_1_top_merchants_by_city_month(df))
    q2 = _run_query("question_2", lambda: question_2_avg_sales_by_merchant_state(df))
    q3 = _run_query("question_3", lambda: question_3_top_hours_by_category(df))
    q4 = _run_query("question_4", lambda: question_4_popular_merchants_cities(df))
    q5 = _run_query("question_5", lambda: question_5_new_merchant_advice(df))

    _safe_save(
        [
            (q1, "question_1"),
            (q2, "question_2"),
            (q3, "question_3"),
            (q4.popular_merchants if q4 else None, "question_4_popular_merchants"),
            (q4.category_correlation if q4 else None, "question_4_category_correlation"),
            (q5.cities if q5 else None, "question_5_cities"),
            (q5.categories if q5 else None, "question_5_categories"),
            (q5.monthly_patterns if q5 else None, "question_5_monthly_patterns"),
            (q5.hourly_patterns if q5 else None, "question_5_hourly_patterns"),
            (q5.installment_analysis if q5 else None, "question_5_installment_analysis"),
        ]
    )

    if all([q1, q2, q3, q4, q5]):
        quality_summary = {
            "malformed": malformed_count,
            "invalid_timestamps": invalid_ts_count,
            "outlier_amounts": outlier_count,
            "denied_transactions": denied_count,
            "clean_rows": row_count,
        }
        try:
            generate_report(q1, q2, q3, q4, q5, quality_summary=quality_summary)
        except Exception:
            logger.exception("Report generation failed")
    else:
        logger.warning("Skipping report generation — not all queries succeeded")

    df.unpersist()
    spark.stop()
    logger.info("Pipeline complete. Log saved to: %s", LOG_FILE)


if __name__ == "__main__":
    run()
