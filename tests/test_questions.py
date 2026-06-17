from datetime import datetime

from pyspark.sql import functions as F

from billups_challenge.questions import (
    question_1_top_merchants_by_city_month,
    question_2_avg_sales_by_merchant_state,
    question_3_top_hours_by_category,
    question_4_popular_merchants_cities,
    question_5_new_merchant_advice,
)


def _sample_data(spark):
    return spark.createDataFrame(
        [
            {
                "merchant": "A",
                "city_id": 1,
                "state_id": 1,
                "category": "food",
                "purchase_amount": 100.0,
                "purchase_date": datetime(2017, 10, 15, 13, 0),
                "installments": 1,
            },
            {
                "merchant": "B",
                "city_id": 1,
                "state_id": 1,
                "category": "food",
                "purchase_amount": 200.0,
                "purchase_date": datetime(2017, 10, 15, 14, 0),
                "installments": 2,
            },
            {
                "merchant": "C",
                "city_id": 1,
                "state_id": 2,
                "category": "tech",
                "purchase_amount": 300.0,
                "purchase_date": datetime(2017, 10, 16, 19, 0),
                "installments": 3,
            },
            {
                "merchant": "D",
                "city_id": 2,
                "state_id": 2,
                "category": "tech",
                "purchase_amount": 400.0,
                "purchase_date": datetime(2017, 11, 10, 8, 0),
                "installments": 1,
            },
            {
                "merchant": "E",
                "city_id": 2,
                "state_id": 1,
                "category": "food",
                "purchase_amount": 500.0,
                "purchase_date": datetime(2017, 11, 12, 12, 0),
                "installments": 1,
            },
            {
                "merchant": "F",
                "city_id": 2,
                "state_id": 2,
                "category": "tech",
                "purchase_amount": 600.0,
                "purchase_date": datetime(2017, 11, 20, 19, 0),
                "installments": 2,
            },
        ]
    )


def test_question_1_returns_top_5_per_month_city(spark):
    df = _sample_data(spark)
    result = question_1_top_merchants_by_city_month(df)

    oct_city1 = result.filter((result.city_id == 1) & (result.month == "Oct 2017")).collect()
    assert len(oct_city1) <= 5
    assert oct_city1[0]["merchant"] == "C"


def test_question_1_never_exceeds_5_per_partition(spark):
    df = _sample_data(spark)
    result = question_1_top_merchants_by_city_month(df)

    counts = result.groupBy("month", "city_id").count().collect()
    for row in counts:
        assert row["count"] <= 5


def test_question_2_ordered_by_average_desc(spark):
    df = _sample_data(spark)
    result = question_2_avg_sales_by_merchant_state(df)
    rows = result.collect()

    amounts = [row["average_amount"] for row in rows]
    assert amounts == sorted(amounts, reverse=True)


def test_question_3_returns_max_3_hours_per_category(spark):
    df = _sample_data(spark)
    result = question_3_top_hours_by_category(df)

    counts = result.groupBy("category").count().collect()
    for row in counts:
        assert row["count"] <= 3


def test_question_3_hours_are_valid_military_format(spark):
    df = _sample_data(spark)
    result = question_3_top_hours_by_category(df)

    hours = [row["hour"] for row in result.collect()]
    for h in hours:
        assert len(h) == 4
        assert h.endswith("00")
        assert 0 <= int(h) <= 2300


def test_question_4_popular_has_one_city_per_merchant(spark):
    df = _sample_data(spark)
    result = question_4_popular_merchants_cities(df)

    assert result.popular_merchants.count() > 0
    merchant_counts = result.popular_merchants.groupBy("merchant").count().collect()
    for row in merchant_counts:
        assert row["count"] == 1


def test_question_5_cash_has_no_default_risk(spark):
    df = _sample_data(spark)
    result = question_5_new_merchant_advice(df)

    cash_row = result.installment_analysis.filter(F.col("installments") == 1).collect()[0]

    revenue = cash_row["total_revenue"]
    expected_profit = revenue * 0.25
    assert abs(cash_row["net_profit"] - expected_profit) < 0.01
    assert abs(cash_row["profit_margin"] - 0.25) < 0.001


def test_question_5_installments_have_lower_margin(spark):
    df = _sample_data(spark)
    result = question_5_new_merchant_advice(df)

    cash_row = result.installment_analysis.filter(F.col("installments") == 1).collect()[0]
    installment_row = result.installment_analysis.filter(F.col("installments") == 2).collect()[0]

    assert installment_row["profit_margin"] < cash_row["profit_margin"]


def test_question_5_installment_profit_formula(spark):
    df = _sample_data(spark)
    result = question_5_new_merchant_advice(df)

    row = result.installment_analysis.filter(F.col("installments") == 2).collect()[0]

    revenue = row["total_revenue"]
    expected_collected = revenue * (1 - 0.229) + revenue * 0.229 * 0.5
    expected_cost = revenue * 0.75
    expected_profit = expected_collected - expected_cost

    assert abs(row["revenue_collected"] - expected_collected) < 0.01
    assert abs(row["net_profit"] - expected_profit) < 0.01


def test_question_5_handles_zero_revenue_without_division_error(spark):
    df = spark.createDataFrame(
        [
            {
                "merchant": "A",
                "city_id": 1,
                "state_id": 1,
                "category": "food",
                "purchase_amount": 0.0,
                "purchase_date": datetime(2017, 10, 15, 13, 0),
                "installments": 1,
            },
        ]
    )

    result = question_5_new_merchant_advice(df)
    row = result.installment_analysis.collect()[0]
    assert row["profit_margin"] == 0.0
