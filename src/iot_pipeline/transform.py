"""
this section to describe the notebook 
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (col, lit, current_timestamp, to_timestamp, window , avg)


def add_ingest_metadata(df: DataFrame)-> DataFrame:

    """ Bronze step: stamp every row with ingestion time and source file.

    Uses `_metadata.file_path` rather than the older `input_file_name()` —
    Unity Catalog on shared/serverless compute blocks `input_file_name()`
    outright (UC_COMMAND_NOT_SUPPORTED) and this is the supported replacement.
    """
    return (df
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path")))


def parse_and_window(df: DataFrame, window_duration: str ="5 minutes", 
                     watermark_duration: str = "10 minutes") -> DataFrame:
    
    """
    Silver step: casts the raw string timestamp to a real timestamp, applies a
    watermark (matters once this runs as a genuinely continuous stream — see
    Part 18 in the guide), and computes 5-minute rolling averages per device.
    """
    return (df
            .withColumn("event_time", to_timestamp("timestamp"))
            .withWatermark("event_time", watermark_duration)
            .groupBy(window("event_time", window_duration), "device")
            .agg(
                avg("temperature").alias("avg_temp"),
                avg("humidity").alias("avg_humidity")
            ))

def detect_alerts(df: DataFrame, threashold: float = 30) -> DataFrame:
    """
    this section to describe 
    """
    return (df
        .filter(col("avg_temp") > threashold)
        .withColumn("alert", lit("HIGH_TEMP")))
                         
