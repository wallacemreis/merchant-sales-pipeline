from datetime import datetime

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import types as T

from billups_challenge.ingestion import (
    clean_transactions,
    deduplicate_merchants,
    deduplicate_transactions,
    filter_invalid_timestamps,
    filter_outlier_amounts,
    quarantine_malformed_rows,
    read_transactions,
    validate_schema,
)

TRANSACTIONS_TEST_SCHEMA = T.StructType(
    [
        T.StructField("merchant_id", T.StringType(), False),
        T.StructField("category", T.StringType(), True),
        T.StructField("purchase_amount", T.DoubleType(), False),
    ]
)


def test_validate_schema_passes_with_valid_columns(spark):
    df = spark.createDataFrame([{"merchant_id": "M1", "purchase_amount": 100.0}])
    result = validate_schema(df, ["merchant_id", "purchase_amount"], "test")
    assert result.count() == 1


def test_validate_schema_raises_on_missing_columns(spark):
    df = spark.createDataFrame([{"merchant_id": "M1"}])
    with pytest.raises(ValueError, match="Missing columns"):
        validate_schema(df, ["merchant_id", "missing_col"], "test")


@pytest.mark.parametrize("file_format", ["parquet", "csv"])
def test_read_transactions_supported_formats(spark, tmp_path, file_format):
    df = spark.createDataFrame([{"merchant_id": "M1", "purchase_amount": 1.0, "category": "food"}])
    path = str(tmp_path / f"transactions.{file_format}")

    if file_format == "parquet":
        df.write.parquet(path)
    else:
        df.write.option("header", "true").csv(path)

    result = read_transactions(spark, path)
    assert result.count() == 1


def test_read_transactions_raises_on_unsupported_format(spark):
    with pytest.raises(ValueError, match="Unsupported file format"):
        read_transactions(spark, "/path/file.json")


def test_read_transactions_raises_on_missing_file(spark, tmp_path):
    path = str(tmp_path / "nonexistent.parquet")
    with pytest.raises(FileNotFoundError, match="Dataset not found"):
        read_transactions(spark, path)


def test_quarantine_separates_valid_from_malformed(spark):
    df = spark.createDataFrame(
        [
            Row(merchant_id="M1", purchase_amount=10.0, city_id=1),
            Row(merchant_id=None, purchase_amount=20.0, city_id=2),
            Row(merchant_id="M3", purchase_amount=None, city_id=None),
        ],
        schema=T.StructType(
            [
                T.StructField("merchant_id", T.StringType(), True),
                T.StructField("purchase_amount", T.DoubleType(), True),
                T.StructField("city_id", T.StringType(), True),
            ]
        ),
    )

    valid, malformed = quarantine_malformed_rows(df, ["merchant_id", "purchase_amount"])
    assert valid.count() == 1
    assert malformed.count() == 2


def test_clean_fills_merchant_name_and_null_category(spark):
    transactions = spark.createDataFrame(
        [
            {"merchant_id": "M1", "category": "food", "purchase_amount": 10.0},
            {"merchant_id": "M2", "category": None, "purchase_amount": 20.0},
        ]
    )
    merchants = spark.createDataFrame([{"merchant_id": "M1", "merchant_name": "Merchant A"}])

    result = clean_transactions(transactions, merchants)
    rows = result.orderBy("merchant_id").collect()

    assert rows[0]["merchant"] == "Merchant A"
    assert rows[1]["merchant"] == "M2"
    assert rows[1]["category"] == "Unknown category"


def test_clean_no_null_merchants(spark):
    transactions = spark.createDataFrame(
        [
            {"merchant_id": "M1", "category": "x", "purchase_amount": 1.0},
            {"merchant_id": "M2", "category": "y", "purchase_amount": 2.0},
        ]
    )
    merchants = spark.createDataFrame([{"merchant_id": "NONE", "merchant_name": "NoMatch"}])

    result = clean_transactions(transactions, merchants)
    assert result.filter(F.col("merchant").isNull()).count() == 0


def test_clean_no_null_categories(spark):
    transactions = spark.createDataFrame(
        [
            Row(merchant_id="M1", category=None, purchase_amount=1.0),
            Row(merchant_id="M2", category=None, purchase_amount=2.0),
        ],
        schema=TRANSACTIONS_TEST_SCHEMA,
    )
    merchants = spark.createDataFrame([{"merchant_id": "M1", "merchant_name": "A"}])

    result = clean_transactions(transactions, merchants)
    assert result.filter(F.col("category").isNull()).count() == 0


@given(
    merchant_ids=st.lists(st.text(min_size=1, max_size=10, alphabet="abcdef0123456789"), min_size=1, max_size=5),
    categories=st.lists(
        st.one_of(st.none(), st.text(min_size=1, max_size=5, alphabet="abcde")), min_size=1, max_size=5
    ),
)
@settings(max_examples=10, deadline=None)
def test_clean_hypothesis_no_nulls_in_output(spark, merchant_ids, categories):
    size = min(len(merchant_ids), len(categories))
    merchant_ids = merchant_ids[:size]
    categories = categories[:size]

    rows = [Row(merchant_id=mid, category=cat, purchase_amount=1.0) for mid, cat in zip(merchant_ids, categories)]
    transactions = spark.createDataFrame(rows, schema=TRANSACTIONS_TEST_SCHEMA)
    merchants = spark.createDataFrame([{"merchant_id": "no_match", "merchant_name": "X"}])

    result = clean_transactions(transactions, merchants)

    assert result.filter(F.col("merchant").isNull()).count() == 0
    assert result.filter(F.col("category").isNull()).count() == 0
    assert result.count() == size


def test_clean_no_row_explosion_with_duplicate_merchants(spark):
    transactions = spark.createDataFrame(
        [
            {"merchant_id": "M1", "category": "food", "purchase_amount": 100.0},
            {"merchant_id": "M2", "category": "tech", "purchase_amount": 200.0},
        ]
    )
    merchants = spark.createDataFrame(
        [
            {"merchant_id": "M1", "merchant_name": "Name A"},
            {"merchant_id": "M1", "merchant_name": "Name B"},
            {"merchant_id": "M1", "merchant_name": "Name C"},
            {"merchant_id": "M2", "merchant_name": "Name D"},
        ]
    )

    result = clean_transactions(transactions, merchants)
    assert result.count() == 2


def test_deduplicate_removes_exact_duplicates(spark):
    df = spark.createDataFrame(
        [
            {
                "customer_id": "C1",
                "merchant_id": "M1",
                "purchase_date": datetime(2017, 10, 1, 12, 0),
                "purchase_amount": 100.0,
                "installments": 1,
                "city_id": 1,
            },
            {
                "customer_id": "C1",
                "merchant_id": "M1",
                "purchase_date": datetime(2017, 10, 1, 12, 0),
                "purchase_amount": 100.0,
                "installments": 1,
                "city_id": 1,
            },
            {
                "customer_id": "C2",
                "merchant_id": "M2",
                "purchase_date": datetime(2017, 10, 2, 14, 0),
                "purchase_amount": 200.0,
                "installments": 2,
                "city_id": 2,
            },
        ]
    )

    result = deduplicate_transactions(df)
    assert result.count() == 2


def test_filter_invalid_timestamps_removes_out_of_range(spark):
    schema = T.StructType(
        [
            T.StructField("merchant_id", T.StringType(), True),
            T.StructField("purchase_date", T.TimestampType(), True),
            T.StructField("purchase_amount", T.DoubleType(), True),
        ]
    )
    df = spark.createDataFrame(
        [
            Row(merchant_id="M1", purchase_date=datetime(2017, 5, 1), purchase_amount=10.0),
            Row(merchant_id="M2", purchase_date=datetime(2035, 1, 1), purchase_amount=20.0),
            Row(merchant_id="M3", purchase_date=datetime(1999, 12, 31), purchase_amount=30.0),
        ],
        schema=schema,
    )

    valid, invalid = filter_invalid_timestamps(df)
    assert valid.count() == 1
    assert invalid.count() == 2


def test_filter_outlier_amounts_removes_negatives_and_zeros(spark):
    df = spark.createDataFrame(
        [
            {"merchant_id": "M1", "purchase_amount": 100.0},
            {"merchant_id": "M2", "purchase_amount": -50.0},
            {"merchant_id": "M3", "purchase_amount": 0.0},
        ]
    )

    valid, outliers = filter_outlier_amounts(df)
    assert valid.count() == 1
    assert outliers.count() == 2


def test_filter_outlier_amounts_removes_extreme_values(spark):
    df = spark.createDataFrame(
        [
            {"merchant_id": "M1", "purchase_amount": 500.0},
            {"merchant_id": "M2", "purchase_amount": 2_000_000.0},
        ]
    )

    valid, outliers = filter_outlier_amounts(df)
    assert valid.count() == 1
    assert outliers.count() == 1


def test_filter_outlier_amounts_boundary_at_max(spark):
    df = spark.createDataFrame(
        [
            {"merchant_id": "M1", "purchase_amount": 1_000_000.0},
            {"merchant_id": "M2", "purchase_amount": 1_000_000.01},
        ]
    )

    valid, outliers = filter_outlier_amounts(df)
    assert valid.count() == 1
    assert valid.collect()[0]["merchant_id"] == "M1"


def test_deduplicate_merchants_keeps_one_per_id(spark):
    df = spark.createDataFrame(
        [
            {"merchant_id": "M1", "merchant_name": "Alpha"},
            {"merchant_id": "M1", "merchant_name": "Beta"},
            {"merchant_id": "M2", "merchant_name": "Gamma"},
        ]
    )

    result = deduplicate_merchants(df)
    assert result.count() == 2
    m1_row = result.filter(F.col("merchant_id") == "M1").collect()[0]
    assert m1_row["merchant_name"] == "Alpha"


def test_deduplicate_merchants_prefers_non_null_name(spark):
    df = spark.createDataFrame(
        [
            Row(merchant_id="M1", merchant_name=None),
            Row(merchant_id="M1", merchant_name="Valid Name"),
        ],
        schema=T.StructType(
            [
                T.StructField("merchant_id", T.StringType(), True),
                T.StructField("merchant_name", T.StringType(), True),
            ]
        ),
    )

    result = deduplicate_merchants(df)
    assert result.count() == 1
    assert result.collect()[0]["merchant_name"] == "Valid Name"


def test_read_dataframe_csv_handles_utf8_characters(spark, tmp_path):
    from billups_challenge.ingestion import read_dataframe

    path = str(tmp_path / "merchants_utf8.csv")
    df = spark.createDataFrame(
        [
            {"merchant_id": "M1", "merchant_name": "Häagen-Dazs"},
            {"merchant_id": "M2", "merchant_name": "Ström & Björk"},
            {"merchant_id": "M3", "merchant_name": "Señor Tacos"},
        ]
    )
    df.write.option("header", "true").option("encoding", "UTF-8").csv(path)

    result = read_dataframe(spark, path)
    names = [row["merchant_name"] for row in result.orderBy("merchant_id").collect()]

    assert "Häagen-Dazs" in names
    assert "Ström & Björk" in names
    assert "Señor Tacos" in names


def test_read_dataframe_csv_handles_commas_in_quoted_fields(spark, tmp_path):
    import os

    from billups_challenge.ingestion import read_dataframe

    csv_content = 'merchant_id,merchant_name\nM1,"Merchant, with comma"\nM2,"Another, complex, name"\n'
    csv_path = str(tmp_path / "quoted.csv")
    with open(csv_path, "w", encoding="utf-8") as f:
        f.write(csv_content)

    result = read_dataframe(spark, os.path.dirname(csv_path) + "/quoted.csv")
    rows = result.orderBy("merchant_id").collect()

    assert rows[0]["merchant_name"] == "Merchant, with comma"
    assert rows[1]["merchant_name"] == "Another, complex, name"
