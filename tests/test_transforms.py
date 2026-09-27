"""
Unit tests for iot_pipeline.transforms — run locally or in a Databricks notebook,
no live stream required.
"""

import pytest
from pyspark.sql import SparkSession
from iot_pipeline.transforms import parse_and_window, detect_alerts


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("test").getOrCreate()


def test_parse_and_window_groups_same_window(spark):
    df = spark.createDataFrame([
        ("sensor_1", "2024-01-01 08:00:00", 20.0, 40.0),
        ("sensor_1", "2024-01-01 08:03:00", 22.0, 42.0),
    ], ["device_id", "timestamp", "temperature", "humidity"])

    result = parse_and_window(df)
    assert result.count() == 1
    assert result.collect()[0]["avg_temp"] == 21.0


def test_parse_and_window_separates_different_windows(spark):
    df = spark.createDataFrame([
        ("sensor_1", "2024-01-01 08:00:00", 20.0, 40.0),
        ("sensor_1", "2024-01-01 08:07:00", 25.0, 45.0),
    ], ["device_id", "timestamp", "temperature", "humidity"])

    result = parse_and_window(df)
    assert result.count() == 2


def test_detect_alerts_filters_by_threshold(spark):
    df = spark.createDataFrame([
        ("sensor_1", 35.0),
        ("sensor_2", 22.0),
    ], ["device_id", "avg_temp"])

    result = detect_alerts(df)
    assert result.count() == 1
    row = result.collect()[0]
    assert row["device_id"] == "sensor_1"
    assert row["alert_type"] == "HIGH_TEMP"