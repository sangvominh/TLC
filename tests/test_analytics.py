"""Hand-counted Phase 3 fixtures; no project HDFS data is modified."""
import csv
from datetime import datetime
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest

from pyspark.sql import SparkSession, functions as F, types as T
from analyze import (SQL_NAMES, ZONE_SCHEMA, prepare_trips, query_kpis, validate_results,
                     write_csv_file, check_zones)

SQL_DIR = "/opt/spark/work-dir/sql/phase3"
SCHEMA = "tpep_pickup_datetime timestamp, PULocationID long, payment_type long, fare_amount double, total_amount double, tip_amount double, trip_distance double, passenger_count int, RatecodeID int"


class AnalyticsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.appName("TLC-Phase-3-Tests")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def fixture(self):
        values = [
            (datetime(2023, 1, 6, 10), 1, 1, 10., 11., 1., 1., None, 99),
            (datetime(2023, 1, 7, 11), 1, 1, 30., 39., 9., 3., None, 1),
            (datetime(2023, 1, 8, 10), 2, 2, 20., 20., 0., 5., 2, 1),
            (datetime(2023, 2, 1, 10), 999, 1, 40., 40., None, 7., None, 1),
            (datetime(2023, 1, 9, 11), 2, 1, 10., -2., 2., 2., 1, 1),
            (datetime(2023, 1, 9, 12), 999, None, 10., None, None, 4., None, 1),
        ]
        data = self.spark.createDataFrame(values, SCHEMA)
        data = (data.withColumn("trip_duration_min", F.lit(30.))
                .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
                .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
                .withColumn("pickup_month", F.month("tpep_pickup_datetime"))
                .withColumn("is_weekend", F.dayofweek("tpep_pickup_datetime").isin(1, 7)))
        zones = self.spark.createDataFrame([(1, "B1", "Zone1", "Yellow"), (2, "B2", "Zone2", "Yellow")], ZONE_SCHEMA)
        return data, zones

    def test_all_kpis_and_physical_csv(self):
        source, zones = self.fixture()
        trips, profile = prepare_trips(source, zones)
        try:
            self.assertEqual((profile["input_rows"], profile["null_passenger_rows"], profile["ratecode_99_rows"]), (6, 4, 1))
            self.assertEqual([(r["pickup_month"], r["count"]) for r in profile["months"]], [(1, 5), (2, 1)])
            self.assertEqual(trips.filter("zone_match_status = 'unmatched'").count(), 2)
            frames = query_kpis(self.spark, trips, SQL_DIR)
            rows = {name: [r.asDict() for r in frame.collect()] for name, frame in frames.items()}
            self.assertTrue(all(validate_results(rows, profile).values()))
            hourly = {r["pickup_hour"]: r for r in rows[SQL_NAMES[0]]}
            self.assertEqual(hourly[10]["recorded_revenue_usd"], Decimal("71"))
            self.assertEqual(hourly[11]["recorded_revenue_usd"], Decimal("37"))
            self.assertIsNone(hourly[12]["recorded_revenue_usd"])
            self.assertEqual(hourly[11]["negative_total_count"], 1)
            self.assertEqual([r["PULocationID"] for r in rows[SQL_NAMES[1]]], [1, 2, 999])
            payment = {r["payment_type"]: r for r in rows[SQL_NAMES[2]]}
            self.assertEqual(payment[1]["trip_count"], 4)
            self.assertEqual(payment[1]["valid_tip_records"], 3)
            self.assertAlmostEqual(float(payment[1]["tip_rate_pct"]), 24.)
            self.assertEqual(payment[1]["tipped_trip_pct"], 100.)
            self.assertIsNone(payment[2]["tip_rate_pct"])
            self.assertIsNone(payment[None]["tip_rate_pct"])
            days = {r["is_weekend"]: r for r in rows[SQL_NAMES[3]]}
            self.assertEqual((days[False]["trip_count"], days[False]["observed_days"]), (4, 3))
            self.assertEqual((days[True]["trip_count"], days[True]["observed_days"]), (2, 2))
            distances = {r["PULocationID"]: r["avg_distance_miles"] for r in rows[SQL_NAMES[4]]}
            self.assertEqual(distances, {1: 2., 2: 3.5, 999: 5.5})
            with tempfile.TemporaryDirectory(prefix="tlc-phase3-test-") as folder:
                for name, frame in frames.items():
                    write_csv_file(self.spark, frame, Path(folder).as_uri(), name)
                    with (Path(folder) / (name + ".csv")).open(newline="", encoding="utf-8") as handle:
                        reader = csv.DictReader(handle)
                        self.assertEqual(reader.fieldnames, frame.columns)
                        self.assertEqual(len(list(reader)), len(rows[name]))
                self.assertEqual(len(list(Path(folder).glob("*.csv"))), 5)
            broken = dict(rows)
            broken[SQL_NAMES[0]] = []
            with self.assertRaisesRegex(ValueError, "conservation"):
                validate_results(broken, profile)
        finally:
            trips.unpersist()

    def test_duplicate_and_null_lookup_keys_fail(self):
        _, zones = self.fixture()
        with self.assertRaisesRegex(ValueError, "duplicate"):
            check_zones(zones.union(zones))
        bad = self.spark.createDataFrame([(None, "B", "Z", None)], ZONE_SCHEMA)
        with self.assertRaisesRegex(ValueError, "null/invalid"):
            check_zones(bad)

    def test_invalid_features_and_empty_source_fail(self):
        source, zones = self.fixture()
        with self.assertRaisesRegex(ValueError, "contract"):
            prepare_trips(source.withColumn("pickup_hour", F.lit(0)), zones)
        with self.assertRaisesRegex(ValueError, "contract"):
            prepare_trips(source.limit(0), zones)

    def test_optional_nonfinite_money_and_negative_tip(self):
        source, zones = self.fixture()
        source = source.withColumn("total_amount", F.lit(float("nan"))).withColumn("tip_amount", F.lit(float("inf")))
        trips, profile = prepare_trips(source, zones)
        try:
            self.assertIsNone(profile["recorded_revenue_usd"])
            self.assertEqual(profile["missing_or_invalid_tip_rows"], 6)
        finally:
            trips.unpersist()
        trips, profile = prepare_trips(source.withColumn("tip_amount", F.lit(-1.)), zones)
        try:
            self.assertEqual(profile["negative_tip_rows"], 6)
            result = query_kpis(self.spark, trips, SQL_DIR)[SQL_NAMES[2]].collect()
            self.assertTrue(all(r.tip_rate_pct is None for r in result))
        finally:
            trips.unpersist()


if __name__ == "__main__":
    unittest.main(verbosity=2)
