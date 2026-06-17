from unittest.mock import patch


def test_save_result_writes_csv(spark, tmp_path):
    from billups_challenge.main import save_result

    df = spark.createDataFrame([{"merchant": "A", "amount": 100.0}])

    with patch("billups_challenge.main.OUTPUT_FORMAT", "csv"), patch("billups_challenge.main.OUTPUT_DIR", tmp_path):
        save_result(df, "test_output")

    output_dir = tmp_path / "test_output"
    assert output_dir.exists()
    csv_files = list(output_dir.glob("*.csv"))
    assert len(csv_files) == 1
