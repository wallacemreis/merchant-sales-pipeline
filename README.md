# Billups Data Engineering Challenge

[![CI](https://github.com/wallacemreis/merchant-sales-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/wallacemreis/merchant-sales-pipeline/actions/workflows/ci.yml)

PySpark solution for the Billups Sr Data Engineer technical challenge. Processes historical transaction and merchant datasets to answer 5 analytical questions about merchant sales patterns.

## Requirements

- Python 3.12+
- Java 17 (for PySpark)
- Docker (optional, for containerized execution)

## Setup

```bash
poetry install
```

## Architecture

```text
┌─────────────────────────────────────────────────────────┐
│                      main.py                            │
│                  (pipeline entry point)                   │
├──────────────┬───────────────────┬──────────────────────┤
│  ingestion   │    questions      │      config          │
│              │                   │                      │
│ - read_*()   │ - question_1()    │ - paths (env vars)   │
│ - validate() │ - question_2()    │ - output format      │
│ - dedup()    │ - question_3()    │ - outlier thresholds │
│ - filter()   │ - question_4()    │                      │
│ - clean()    │ - question_5()    │      session         │
│              │                   │ - create_spark_*()   │
└──────┬───────┴────────┬──────────┴──────────────────────┘
       │                │
       ▼                ▼
 ┌───────────┐   ┌────────────┐
 │ data/     │   │  output/   │
 │ (input)   │   │  (results) │
 └───────────┘   └────────────┘
```

### Layers

1. **Config** (`config.py`) - All paths and thresholds via environment variables.
2. **Session** (`session.py`) - SparkSession factory.
3. **Ingestion** (`ingestion.py`) - Data quality pipeline:
   - Multi-format reader (parquet/csv) with explicit encoding (UTF-8)
   - Schema validation on load
   - Quarantine of malformed rows (nulls in required columns)
   - Deduplication of transactions (idempotent, by business key)
   - Timestamp validation (rejects dates outside [2000, 2030))
   - Outlier filtering (configurable min/max thresholds)
   - Merchant deduplication (prevents join explosion on many-to-many)
   - Transform chaining for cleaning (merchant name fallback + null category fill)
4. **Questions** (`questions.py`) - Pure functions that take a clean DataFrame and return typed results.
5. **Report** (`report.py`) - Writes `output/REPORT.md` with tables and analysis.
6. **Main** (`main.py`) - Runs the full pipeline; if one query fails the rest still run.

### Design Decisions

- **Separation of concerns:** ingestion separated from analytical queries. Each question is its own function, testable in isolation.
- **Lazy transformations:** quality functions avoid `count()` inside transformations. Materialization only happens when saving quarantine files.
- **Deterministic outputs:** window functions use explicit ordering for stable results.
- **Schema registry:** `DatasetSpec` binds schema + required columns + dataset name together.
- **Write strategy:** output format (csv/parquet) selectable via env var, implemented as a dict of callables.
- **Fault tolerance:** if one query fails the rest still run and results are logged.
- **Typed results:** Q4 and Q5 return dataclasses instead of raw dicts.
- **Spark tuning for local mode:** `driver.memory=4g` (default 1g is insufficient for joins + windows on a 300MB parquet) and `shuffle.partitions=8` (matches `local[*]` cores; default 200 is designed for clusters).

## Running

### Local (Poetry)

```bash
poetry run task run
```

### Spark Submit

```bash
spark-submit \
  --master "local[*]" \
  --conf "spark.driver.memory=4g" \
  billups_challenge/main.py
```

### Docker

```bash
poetry run task docker-build
poetry run task docker-run
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BILLUPS_DATA_DIR` | `./data` | Directory containing input files |
| `BILLUPS_TRANSACTIONS_FILE` | `historical_transactions.parquet` | Transactions filename |
| `BILLUPS_MERCHANTS_FILE` | `merchants.csv` | Merchants filename |
| `BILLUPS_OUTPUT_DIR` | `./output` | Directory for results |
| `BILLUPS_OUTPUT_FORMAT` | `csv` | Output format (`csv` or `parquet`) |
| `BILLUPS_PURCHASE_AMOUNT_MIN` | `0.0` | Minimum valid purchase amount |
| `BILLUPS_PURCHASE_AMOUNT_MAX` | `1000000.0` | Maximum valid purchase amount |
| `BILLUPS_LOG_FILE` | `./output/pipeline.log` | Log file path for post-execution review |

The `data/` directory is not versioned (files are provided separately by the challenge). Place the following files there before running:

- `historical_transactions.parquet`
- `merchants.csv`

## Testing

### Local

```bash
poetry run task test
```

### Docker

```bash
poetry run task docker-test
```

### Test Strategy

Unit tests prove correctness for known inputs; property-based tests (Hypothesis) verify invariants hold against randomized inputs we might not think to write manually.

- **Unit tests** for each analytical function with controlled sample data
- **Property-based tests** (Hypothesis) for data cleaning invariants: no null merchants, no null categories, row count preservation
- **Parametrized tests** for file formats, unsupported extensions, boundary values
- **Data quality tests** for deduplication, timestamp validation, outlier filtering, encoding handling, many-to-many join prevention
- **Business logic tests** for Q5e installment profit formula (flat default rate, cash vs installment margins)

## Available Tasks

```bash
poetry run task test          # Run tests with coverage
poetry run task lint          # Check formatting and linting
poetry run task fmt           # Auto-format code
poetry run task run           # Execute the pipeline locally
poetry run task docker-build  # Build Docker image
poetry run task docker-test   # Run tests in Docker
poetry run task docker-run    # Run pipeline in Docker
poetry run task docker-clean  # Prune Docker cache/volumes
```

## Project Structure

```text
billups_challenge/
├── __init__.py
├── config.py          # Paths, thresholds, env configuration
├── ingestion.py       # Reading, validation, quality checks, cleaning
├── main.py            # Pipeline entry point
├── questions.py       # Analytical queries (Q1-Q5)
├── report.py          # Markdown report generation
└── session.py         # SparkSession factory
tests/
├── conftest.py        # Shared SparkSession fixture
├── test_ingestion.py  # Data quality + hypothesis + encoding
├── test_main.py       # Output writing
└── test_questions.py  # Query correctness + business logic
```

## Data Quality Pipeline

```text
Raw Input
    │
    ▼
Schema Validation ──── ✗ → raise ValueError
    │
    ▼
Quarantine Nulls ───── malformed → quarantine_malformed/
    │
    ▼
Deduplication ──────── removes exact business duplicates
    │
    ▼
Timestamp Filter ───── invalid → quarantine_invalid_timestamps/
    │
    ▼
Outlier Filter ─────── outliers → quarantine_outlier_amounts/
    │
    ▼
Denied Filter ──────── denied (authorized_flag != Y) → quarantine_denied/
    │
    ▼
Clean (join + fill) ── merchant names + "Unknown category"
    │
    ▼
Analytical Queries ──→ output/REPORT.md (with data quality summary)
```

## Data Cleaning Rules

1. Transactions without a matching merchant use `merchant_id` as the merchant name
2. Null categories are replaced with "Unknown category"
3. Schema validation on load raises immediately on missing columns
4. Duplicate merchants are resolved (one per `merchant_id`, preferring non-null names)
5. Duplicate transactions are removed by business key (customer, merchant, date, amount, installments, city)
6. Timestamps outside [2000, 2030) are quarantined
7. Purchase amounts outside configurable bounds are quarantined
8. Denied transactions (`authorized_flag != 'Y'`) are filtered out (only completed sales are analyzed)

## Questions Answered

| # | Description | Output |
| --- | --- | --- |
| 1 | Top 5 merchants by purchase_amount per month per city | Month, City, Merchant, Purchase Total, No of sales |
| 2 | Average purchase_amount per merchant per state (largest first) | Merchant, State ID, Average Amount |
| 3 | Top 3 hours with largest purchase_amount per product category | Product Category, Hour |
| 4 | Cities where popular merchants are located + city-category correlation | `MerchantCityAnalysis` dataclass (two DataFrames) |
| 5 | Business advice for a new merchant | `NewMerchantAdvice` dataclass (cities, categories, monthly/hourly patterns, installment analysis) |

### Implementation Rationale

**Q1** - Groups by (month, city, merchant), sums purchase_amount and counts transactions. Uses `row_number()` partitioned by (month, city) to pick top 5 per partition. Month formatted as "MMM yyyy" via `date_format`.

**Q2** - `groupBy("merchant", "state_id")` + `avg("purchase_amount")`, ordered desc. One row per merchant-state pair.

**Q3** - Groups by (category, hour), sums purchase_amount, then uses `row_number()` per category to pick the 3 peak hours. Hour output in 4-digit military format (e.g. "0800", "1300") via `lpad(concat(hour, "00"), 4, "0")`.

**Q4** - Two parts. First: each merchant can sell in multiple cities, so we find the "primary city" (city with most transactions) using a window function, then rank merchants by total transaction count and take top 20. Second: for each city, count transactions per category and show top 3 (answers the correlation question).

**Q5** - Five sub-analyses: (a) top 10 cities by revenue, (b) top 10 categories by revenue, (c) monthly revenue distribution, (d) hourly revenue distribution, (e) installment profitability model (cash vs installment margins under flat 22.9% default rate, 25% gross profit).

## Assumptions

### Data Quality (not specified in the challenge)

- **Deduplication by business key:** We deduplicate transactions by (customer_id, merchant_id, purchase_date, purchase_amount, installments, city_id). Verified that the current dataset has zero duplicates, but the step guards against re-ingestion issues common in production parquet dumps.
- **Denied transactions filtered:** Only `authorized_flag == 'Y'` transactions are analyzed. The PDF doesn't mention this flag, but 8.6% of records are denied; including incomplete sales would distort revenue/volume metrics.
- **Timestamp range [2000, 2030):** Rejects dates that are clearly data errors. The dataset has zero invalid timestamps.
- **Outlier bounds (0, 1M]:** Purchase amounts <= 0 or > 1M are quarantined. The dataset has zero outliers with default bounds.
- **Merchant deduplication:** The merchants.csv has 63 duplicate merchant_ids (334,696 rows, 334,633 unique). We keep one per merchant_id, preferring non-null names.

### Question 5e (Installments)

- 25% of purchase_amount is gross profit to merchants
- 22.9% credit default rate applied as a flat probability (per the challenge's "simplistic" wording)
- Defaulters pay half of the total amount before defaulting
- Equal installment amounts across the payment plan
- Cash sales (installments=0 or 1) have no default risk, full 25% margin
- `installments=0` treated as cash (no installment plan)

In practice, 22.9%/month would compound (`1 - (1 - 0.229)^N`), making a 12-month plan ~95% likely to default. We use the flat model because the challenge says "simplistic assumption". Result: ~13.55% margin on installments vs 25% on cash.

## CI/CD

GitHub Actions runs lint + tests on every push/PR to `main`. The workflow:

1. Sets up Python 3.12 + Java 17 (required for PySpark)
2. Installs dependencies via Poetry
3. Runs `poetry run task lint` (ruff check + format)
4. Runs `poetry run task test` (pytest with coverage)
