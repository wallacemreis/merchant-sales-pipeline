from dataclasses import dataclass

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


@dataclass
class MerchantCityAnalysis:
    """Q4 result: popular merchants with their primary city and category correlation."""

    popular_merchants: DataFrame
    category_correlation: DataFrame


@dataclass
class NewMerchantAdvice:
    """Q5 result: recommended cities, categories, time patterns, and installment model."""

    cities: DataFrame
    categories: DataFrame
    monthly_patterns: DataFrame
    hourly_patterns: DataFrame
    installment_analysis: DataFrame


def question_1_top_merchants_by_city_month(df: DataFrame) -> DataFrame:
    """Rank the top 5 merchants by purchase_amount for each (month, city) pair."""
    monthly = df.withColumn("month", F.date_format(F.col("purchase_date"), "MMM yyyy"))

    aggregated = monthly.groupBy("month", "city_id", "merchant").agg(
        F.sum("purchase_amount").alias("purchase_total"),
        F.count("*").alias("no_of_sales"),
    )

    window = Window.partitionBy("month", "city_id").orderBy(F.col("purchase_total").desc())

    ranked = aggregated.withColumn("rank", F.row_number().over(window)).filter(F.col("rank") <= 5).drop("rank")

    return ranked.orderBy("month", "city_id", F.col("purchase_total").desc())


def question_2_avg_sales_by_merchant_state(df: DataFrame) -> DataFrame:
    """Compute average purchase_amount per (merchant, state), ordered desc."""
    return (
        df.groupBy("merchant", "state_id")
        .agg(F.avg("purchase_amount").alias("average_amount"))
        .orderBy(F.col("average_amount").desc())
    )


def question_3_top_hours_by_category(df: DataFrame) -> DataFrame:
    """Find the 3 peak hours by total purchase_amount for each category."""
    hourly = df.withColumn("hour", F.hour(F.col("purchase_date")))

    aggregated = hourly.groupBy("category", "hour").agg(
        F.sum("purchase_amount").alias("total_amount"),
    )

    window = Window.partitionBy("category").orderBy(F.col("total_amount").desc())

    ranked = aggregated.withColumn("rank", F.row_number().over(window)).filter(F.col("rank") <= 3).drop("rank")

    return (
        ranked.withColumn("hour", F.lpad(F.concat(F.col("hour"), F.lit("00")), 4, "0"))
        .select("category", "hour")
        .orderBy("category", "hour")
    )


def question_4_popular_merchants_cities(df: DataFrame, top_n: int = 20) -> MerchantCityAnalysis:
    """Identify which cities concentrate the most popular merchants."""
    merchant_total = df.groupBy("merchant").agg(F.count("*").alias("total_transactions"))

    merchant_by_city = df.groupBy("merchant", "city_id").agg(F.count("*").alias("city_transactions"))

    city_window = Window.partitionBy("merchant").orderBy(F.col("city_transactions").desc())
    primary_city = (
        merchant_by_city.withColumn("rank", F.row_number().over(city_window)).filter(F.col("rank") == 1).drop("rank")
    )

    popular_merchants = (
        primary_city.join(merchant_total, on="merchant")
        .orderBy(F.col("total_transactions").desc())
        .limit(top_n)
        .select("merchant", "city_id", "total_transactions")
    )

    category_by_city = df.groupBy("city_id", "category").agg(F.count("*").alias("category_count"))

    cat_window = Window.partitionBy("city_id").orderBy(F.col("category_count").desc())
    top_categories_per_city = (
        category_by_city.withColumn("rank", F.row_number().over(cat_window)).filter(F.col("rank") <= 3).drop("rank")
    )

    return MerchantCityAnalysis(
        popular_merchants=popular_merchants,
        category_correlation=top_categories_per_city,
    )


def question_5_new_merchant_advice(df: DataFrame) -> NewMerchantAdvice:
    """Build a data-driven recommendation for a new merchant entering the market."""

    cities = (
        df.groupBy("city_id")
        .agg(
            F.sum("purchase_amount").alias("total_revenue"),
            F.count("*").alias("total_transactions"),
            F.avg("purchase_amount").alias("avg_ticket"),
        )
        .orderBy(F.col("total_revenue").desc())
        .limit(10)
    )

    categories = (
        df.groupBy("category")
        .agg(
            F.sum("purchase_amount").alias("total_revenue"),
            F.count("*").alias("total_transactions"),
        )
        .orderBy(F.col("total_revenue").desc())
        .limit(10)
    )

    monthly_patterns = (
        df.withColumn("month_num", F.month(F.col("purchase_date")))
        .groupBy("month_num")
        .agg(
            F.sum("purchase_amount").alias("total_revenue"),
            F.count("*").alias("total_transactions"),
        )
        .orderBy("month_num")
    )

    hourly_patterns = (
        df.withColumn("hour", F.hour(F.col("purchase_date")))
        .groupBy("hour")
        .agg(
            F.sum("purchase_amount").alias("total_revenue"),
            F.count("*").alias("total_transactions"),
        )
        .orderBy("hour")
    )

    # 25% gross margin, 22.9% flat default rate on installments, defaulters pay half
    DEFAULT_RATE = 0.229
    GROSS_MARGIN = 0.25
    COST_RATIO = 1 - GROSS_MARGIN

    installment_analysis = (
        df.groupBy("installments")
        .agg(
            F.sum("purchase_amount").alias("total_revenue"),
            F.count("*").alias("total_transactions"),
            F.avg("purchase_amount").alias("avg_amount"),
        )
        .withColumn("is_installment", F.when(F.col("installments") > 1, True).otherwise(False))
        .withColumn(
            "revenue_collected",
            F.when(
                F.col("installments") > 1,
                F.col("total_revenue") * (1 - DEFAULT_RATE) + F.col("total_revenue") * DEFAULT_RATE * 0.5,
            ).otherwise(F.col("total_revenue")),
        )
        .withColumn("cost_of_goods", F.col("total_revenue") * COST_RATIO)
        .withColumn("net_profit", F.col("revenue_collected") - F.col("cost_of_goods"))
        .withColumn(
            "profit_margin",
            F.when(F.col("total_revenue") > 0, F.col("net_profit") / F.col("total_revenue")).otherwise(F.lit(0.0)),
        )
        .orderBy("installments")
    )

    return NewMerchantAdvice(
        cities=cities,
        categories=categories,
        monthly_patterns=monthly_patterns,
        hourly_patterns=hourly_patterns,
        installment_analysis=installment_analysis,
    )
