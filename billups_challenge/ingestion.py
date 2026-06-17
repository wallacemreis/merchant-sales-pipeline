import logging
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from billups_challenge.config import PURCHASE_AMOUNT_MAX, PURCHASE_AMOUNT_MIN

logger = logging.getLogger(__name__)

TRANSACTIONS_SCHEMA = T.StructType(
    [
        T.StructField("customer_id", T.StringType(), True),
        T.StructField("month_lag", T.IntegerType(), True),
        T.StructField("purchase_date", T.TimestampType(), True),
        T.StructField("authorized_flag", T.StringType(), True),
        T.StructField("category", T.StringType(), True),
        T.StructField("installments", T.IntegerType(), True),
        T.StructField("merchant_category_id", T.IntegerType(), True),
        T.StructField("subsector_id", T.IntegerType(), True),
        T.StructField("merchant_id", T.StringType(), True),
        T.StructField("purchase_amount", T.DoubleType(), True),
        T.StructField("city_id", T.IntegerType(), True),
        T.StructField("state_id", T.IntegerType(), True),
    ]
)

MERCHANTS_SCHEMA = T.StructType(
    [
        T.StructField("merchant_name", T.StringType(), True),
        T.StructField("merchant_id", T.StringType(), True),
        T.StructField("merchant_group_id", T.StringType(), True),
        T.StructField("merchant_category_id", T.IntegerType(), True),
        T.StructField("subsector_id", T.IntegerType(), True),
        T.StructField("numerical_1", T.DoubleType(), True),
        T.StructField("numerical_2", T.DoubleType(), True),
        T.StructField("most_recent_sales_range", T.StringType(), True),
        T.StructField("most_recent_purchases_range", T.StringType(), True),
        T.StructField("avg_sales_lag3", T.DoubleType(), True),
        T.StructField("avg_purchases_lag3", T.DoubleType(), True),
        T.StructField("active_months_lag3", T.IntegerType(), True),
        T.StructField("avg_sales_lag6", T.DoubleType(), True),
        T.StructField("avg_purchases_lag6", T.DoubleType(), True),
        T.StructField("active_months_lag6", T.IntegerType(), True),
        T.StructField("avg_sales_lag12", T.DoubleType(), True),
        T.StructField("avg_purchases_lag12", T.DoubleType(), True),
        T.StructField("active_months_lag12", T.IntegerType(), True),
        T.StructField("city_id", T.IntegerType(), True),
        T.StructField("state_id", T.IntegerType(), True),
    ]
)


class DatasetSpec:
    """Group schema, required columns and dataset name together."""

    def __init__(self, name: str, schema: T.StructType, required_columns: list[str]):
        self.name = name
        self.schema = schema
        self.required_columns = required_columns


TRANSACTIONS_SPEC = DatasetSpec(
    name="historical_transactions",
    schema=TRANSACTIONS_SCHEMA,
    required_columns=["merchant_id", "purchase_amount", "purchase_date", "city_id", "state_id"],
)

MERCHANTS_SPEC = DatasetSpec(
    name="merchants",
    schema=MERCHANTS_SCHEMA,
    required_columns=["merchant_id"],
)

MIN_VALID_DATE = datetime(2000, 1, 1)
MAX_VALID_DATE = datetime(2030, 1, 1)


def read_dataframe(spark: SparkSession, path: str, schema: T.StructType | None = None) -> DataFrame:
    """Load a parquet or csv file into a DataFrame, enforcing schema on csv."""
    import os

    path_str = str(path)

    if not path_str.endswith((".parquet", ".csv")):
        raise ValueError(f"Unsupported file format: {path_str}. Expected .parquet or .csv")

    if not os.path.exists(path_str):
        raise FileNotFoundError(f"Dataset not found: {path_str}")

    if path_str.endswith(".parquet"):
        return spark.read.parquet(path_str)

    reader = spark.read.option("header", "true").option("encoding", "UTF-8").option("charToEscapeQuoteEscaping", "\\")
    if schema:
        reader = reader.schema(schema)
    return reader.csv(path_str)


def read_transactions(spark: SparkSession, path: str) -> DataFrame:
    """Load the transactions parquet with enforced schema."""
    return read_dataframe(spark, path, schema=TRANSACTIONS_SCHEMA)


def read_merchants(spark: SparkSession, path: str) -> DataFrame:
    """Load the merchants csv with enforced schema."""
    return read_dataframe(spark, path, schema=MERCHANTS_SCHEMA)


def validate_schema(df: DataFrame, expected_columns: list[str], dataset_name: str) -> DataFrame:
    """Raise ValueError if expected columns are missing."""
    missing = set(expected_columns) - set(df.columns)
    if missing:
        raise ValueError(f"[{dataset_name}] Missing columns: {missing}")
    return df


def quarantine_malformed_rows(df: DataFrame, required_columns: list[str]) -> tuple[DataFrame, DataFrame]:
    """Split into (valid, malformed) based on nulls in required columns."""
    condition = F.lit(True)
    for col_name in required_columns:
        condition = condition & F.col(col_name).isNotNull()

    valid = df.filter(condition)
    malformed = df.filter(~condition)
    return valid, malformed


def deduplicate_transactions(df: DataFrame) -> DataFrame:
    """Drop duplicate transactions by business key."""
    dedup_cols = ["customer_id", "merchant_id", "purchase_date", "purchase_amount", "installments", "city_id"]
    return df.dropDuplicates(dedup_cols)


def filter_invalid_timestamps(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split into (valid, invalid) based on purchase_date within [2000, 2030)."""
    min_ts = F.lit(MIN_VALID_DATE)
    max_ts = F.lit(MAX_VALID_DATE)

    valid_condition = (F.col("purchase_date") >= min_ts) & (F.col("purchase_date") < max_ts)
    return df.filter(valid_condition), df.filter(~valid_condition)


def filter_outlier_amounts(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split into (valid, outliers) based on configurable amount bounds."""
    valid_condition = (F.col("purchase_amount") > F.lit(PURCHASE_AMOUNT_MIN)) & (
        F.col("purchase_amount") <= F.lit(PURCHASE_AMOUNT_MAX)
    )
    return df.filter(valid_condition), df.filter(~valid_condition)


def filter_denied_transactions(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split into (approved, denied) based on authorized_flag."""
    approved = df.filter(F.col("authorized_flag") == "Y")
    denied = df.filter(F.col("authorized_flag") != "Y")
    return approved, denied


def deduplicate_merchants(df: DataFrame) -> DataFrame:
    """Keep one row per merchant_id, preferring non-null names."""
    total = df.count()
    unique = df.select("merchant_id").distinct().count()
    if total != unique:
        logger.warning(
            "Merchants dataset has %d duplicate merchant_ids (%d rows, %d unique). "
            "Keeping first non-null name per merchant_id.",
            total - unique,
            total,
            unique,
        )

    window = Window.partitionBy("merchant_id").orderBy(F.col("merchant_name").asc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")


def _join_merchant_names(merchants: DataFrame):
    """Return a transform that joins merchant names onto transactions."""
    merchant_names = deduplicate_merchants(merchants).select("merchant_id", "merchant_name")

    def _transform(df: DataFrame) -> DataFrame:
        return (
            df.join(merchant_names, on="merchant_id", how="left")
            .withColumn("merchant", F.coalesce(F.col("merchant_name"), F.col("merchant_id")))
            .drop("merchant_name")
        )

    return _transform


def _fill_missing_categories(df: DataFrame) -> DataFrame:
    """Replace null categories with 'Unknown category'."""
    return df.withColumn("category", F.coalesce(F.col("category"), F.lit("Unknown category")))


def clean_transactions(transactions: DataFrame, merchants: DataFrame) -> DataFrame:
    """Apply merchant name join and category fill via transform chaining."""
    return transactions.transform(_join_merchant_names(merchants)).transform(_fill_missing_categories)
