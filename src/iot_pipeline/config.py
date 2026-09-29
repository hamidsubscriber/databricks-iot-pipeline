"""
Central Configuration for the IOT Streaming pipeline (Project 2).
"""
CATALOG = "dev"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.iot_readings_raw"
SILVER_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.iot_reading_5min"
GOLD_TABLE =  f"{CATALOG}.{GOLD_SCHEMA}.iot_alerts"

LANDING_PATH = "/Volumes/dev/bronze/iot_landing"
CHECKPOINT_BASE = "/Volumes/dev/bronze/iot_checkpoints/"

TEMP_ALERT_THRESHOLD = 30