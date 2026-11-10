"""Kiểm thử Phase 2 bằng dữ liệu mẫu riêng, không sửa dữ liệu HDFS của dự án."""

from datetime import datetime, timedelta
import json
from pathlib import Path
import tempfile
import unittest
from urllib.parse import urlparse

from pyspark.sql import SparkSession, functions as F, types as T

from clean import run_phase2
from verify import validate_output


SCHEMA = T.StructType([
    T.StructField("test_id", T.StringType()),
    T.StructField("tpep_pickup_datetime", T.TimestampType()),
    T.StructField("tpep_dropoff_datetime", T.TimestampType()),
    T.StructField("trip_distance", T.DoubleType()),
    T.StructField("fare_amount", T.DoubleType()),
    T.StructField("passenger_count", T.DoubleType()),
    T.StructField("RatecodeID", T.DoubleType()),
    T.StructField("PULocationID", T.IntegerType()),
    T.StructField("DOLocationID", T.IntegerType()),
])


def trip(name, **changes):
    row = dict(test_id=name, tpep_pickup_datetime=datetime(2023, 1, 7, 10),
               tpep_dropoff_datetime=datetime(2023, 1, 7, 10, 30),
               trip_distance=10.0, fare_amount=20.0, passenger_count=2.0,
               RatecodeID=1.0, PULocationID=1, DOLocationID=2)
    row.update(changes)
    return row


class Phase2Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.appName("TLC-Phase-2-Regression")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.ansi.enabled", "true")
                     .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def check_pipeline(self, rows, expected_failed_rules):
        with tempfile.TemporaryDirectory(prefix="tlc-phase2-test-") as directory:
            root = Path(directory)
            raw, output, reports = [f"{root.as_uri()}/{name}"
                                    for name in ("raw", "cleaned", "reports")]
            self.spark.createDataFrame(rows, SCHEMA).write.parquet(raw)
            local_output = root.as_uri() + "/local-cleaned"
            report, report_path = run_phase2(self.spark, raw, output, reports, local_output)
            result = self.spark.read.parquet(output)
            actual = {row.test_id: row.asDict() for row in result.collect()}
            expected_ids = {row["test_id"] for row in rows
                            if row["test_id"] not in expected_failed_rules}
            self.assertEqual(set(actual), expected_ids)
            self.assertEqual(report["input_rows"], len(rows))
            self.assertEqual(report["output_rows"], len(expected_ids))
            before = len(rows)
            for step, audit in enumerate(report["rules"], 1):
                after = sum(expected_failed_rules.get(row["test_id"], 9) > step
                            for row in rows)
                self.assertEqual((audit["rows_in"], audit["rows_out"], audit["rows_removed"]),
                                 (before, after, before - after))
                before = after
            self.assertEqual(sum(item["rows_removed"] for item in report["rules"]),
                             report["rows_removed_total"])
            self.assertEqual(set(result.columns), set(SCHEMA.names) | {
                "trip_duration_min", "pickup_date", "pickup_hour", "pickup_month", "is_weekend"})
            self.assertEqual(result.schema["passenger_count"].dataType, T.IntegerType())
            self.assertEqual(result.schema["RatecodeID"].dataType, T.IntegerType())
            self.assertTrue((root / "cleaned" / "_SUCCESS").exists())
            report_directory = Path(urlparse(report_path).path)
            self.assertTrue((report_directory / "_SUCCESS").exists())
            self.assertEqual(json.loads(next(report_directory.glob("part-*")).read_text()), report)
            self.assertEqual(json.loads(next((root / "reports" / "latest").glob("part-*")).read_text()), report)
            self.assertTrue(report["validation"]["passed"])
            self.assertEqual(report["validation"]["invalid_rows"], 0)
            self.assertEqual(report["validation"]["output_rows"], len(expected_ids))
            self.assertTrue(report["local_validation"]["passed"])
            self.assertEqual(report["local_validation"], report["validation"])
            self.assertEqual(report["local_output_path"], local_output)
            local_rows = self.spark.read.parquet(local_output).collect()
            self.assertEqual({row.test_id: row.asDict() for row in local_rows}, actual)
            self.assertTrue((root / "local-cleaned" / "_SUCCESS").exists())
            return actual, report, [path.name for path in (root / "cleaned").iterdir()]

    def test_rules_boundaries_and_multiple_months(self):
        start = datetime(2023, 1, 7, 10)
        valid_rows = [
            trip("normal"), trip("zero_passenger", passenger_count=0.0),
            trip("over_six", passenger_count=7.0), trip("null_passenger", passenger_count=None),
            trip("six_passengers", passenger_count=6.0), trip("rate_99", RatecodeID=99.0),
            trip("six_hours", tpep_dropoff_datetime=start + timedelta(hours=6), trip_distance=100.0),
            trip("max_distance", tpep_dropoff_datetime=start + timedelta(hours=2), trip_distance=100.0),
            trip("max_speed", trip_distance=40.0),
            trip("fraction_second", tpep_dropoff_datetime=start + timedelta(microseconds=500000),
                 trip_distance=0.001),
            trip("february", tpep_pickup_datetime=datetime(2023, 2, 6, 10),
                 tpep_dropoff_datetime=datetime(2023, 2, 6, 10, 30)),
            trip("year_start", tpep_pickup_datetime=datetime(2023, 1, 1),
                 tpep_dropoff_datetime=datetime(2023, 1, 1, 0, 30)),
            trip("year_end", tpep_pickup_datetime=datetime(2023, 12, 31, 23, 45),
                 tpep_dropoff_datetime=datetime(2024, 1, 1, 0, 15)),
        ]
        invalid = [
            (trip("same_time", tpep_dropoff_datetime=start), 1),
            (trip("backward", tpep_dropoff_datetime=start - timedelta(seconds=1)), 1),
            (trip("null_pickup", tpep_pickup_datetime=None), 1),
            (trip("null_dropoff", tpep_dropoff_datetime=None), 1),
            (trip("too_long", tpep_dropoff_datetime=start + timedelta(hours=6, microseconds=1)), 2),
            (trip("old_year", tpep_pickup_datetime=datetime(2008, 1, 1),
                  tpep_dropoff_datetime=datetime(2008, 1, 1, 0, 30)), 3),
            (trip("next_year", tpep_pickup_datetime=datetime(2024, 1, 1),
                  tpep_dropoff_datetime=datetime(2024, 1, 1, 0, 30)), 3),
            (trip("zero_distance", trip_distance=0.0), 4),
            (trip("negative_distance", trip_distance=-1.0), 4),
            (trip("huge_distance", trip_distance=100.001), 4),
            (trip("null_distance", trip_distance=None), 4),
            (trip("nan_distance", trip_distance=float("nan")), 4),
            (trip("too_fast", trip_distance=40.001), 5),
            (trip("zero_fare", fare_amount=0.0), 6),
            (trip("negative_fare", fare_amount=-1.0), 6),
            (trip("null_fare", fare_amount=None), 6),
            (trip("nan_fare", fare_amount=float("nan")), 6),
            (trip("null_pu", PULocationID=None), 7),
            (trip("null_do", DOLocationID=None), 8),
            (trip("overlap", tpep_dropoff_datetime=start, trip_distance=0.0, fare_amount=0.0), 1),
        ]
        actual, report, paths = self.check_pipeline(
            valid_rows + [row for row, _ in invalid],
            {row["test_id"]: step for row, step in invalid})
        self.assertEqual(actual["zero_passenger"]["passenger_count"], None)
        self.assertEqual(actual["zero_passenger"]["fare_amount"], 20.0)
        self.assertEqual(actual["over_six"]["passenger_count"], None)
        self.assertEqual(actual["six_passengers"]["passenger_count"], 6)
        self.assertEqual(actual["rate_99"]["RatecodeID"], 99)
        self.assertEqual(actual["normal"]["trip_duration_min"], 30.0)
        self.assertTrue(actual["normal"]["is_weekend"])
        self.assertFalse(actual["february"]["is_weekend"])
        self.assertEqual(actual["normal"]["pickup_hour"], 10)
        self.assertEqual(actual["normal"]["pickup_month"], 1)
        self.assertAlmostEqual(actual["fraction_second"]["trip_duration_min"], 0.5 / 60)
        self.assertEqual(report["transformations"]["passenger_count"]["rows_changed_to_null"], 2)
        self.assertEqual(report["transformations"]["passenger_count"]["output_null_rows"], 3)
        self.assertEqual(report["partitioning"], "pickup_month")
        self.assertEqual({name for name in paths if name.startswith("pickup_month=")},
                         {"pickup_month=1", "pickup_month=2", "pickup_month=12"})

    def test_single_month(self):
        _, report, paths = self.check_pipeline([trip("single")], {})
        self.assertEqual(report["partitioning"], "none")
        self.assertFalse(any("=" in name for name in paths))

    def test_all_removed(self):
        _, report, _ = self.check_pipeline([trip("invalid", trip_distance=0.0)], {"invalid": 4})
        self.assertEqual(report["output_rows"], 0)

    def test_empty_input(self):
        _, report, _ = self.check_pipeline([], {})
        self.assertEqual(report["rows_removed_total"], 0)

    def test_reject_corrupt_features(self):
        with tempfile.TemporaryDirectory(prefix="tlc-phase2-corrupt-") as directory:
            path = Path(directory).as_uri() + "/corrupt"
            corrupted = (
                self.spark.createDataFrame([trip("wrong_hour")], SCHEMA)
                .withColumn("passenger_count", F.col("passenger_count").cast("int"))
                .withColumn("RatecodeID", F.col("RatecodeID").cast("int"))
                .withColumn("trip_duration_min", F.lit(30.0))
                .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
                .withColumn("pickup_hour", F.lit(99))
                .withColumn("pickup_month", F.lit(1))
                .withColumn("is_weekend", F.lit(True))
            )
            corrupted.write.parquet(path)
            with self.assertRaises(RuntimeError):
                validate_output(self.spark, path, F.lit(True), F.lit(30.0), 1, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
