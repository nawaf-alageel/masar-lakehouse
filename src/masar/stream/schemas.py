from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

PING_SCHEMA = StructType([
    StructField("trip_id",   StringType()),
    StructField("driver_id", StringType()),
    StructField("city",      StringType()),
    StructField("lat",       DoubleType()),
    StructField("lon",       DoubleType()),
    StructField("event_ts",  TimestampType()),   # event time, from the phone
])

# We run Python with -m, not spark-submit --packages:
#   get_spark("name", kafka=True) adds the Kafka connector jar itself.
#   It must be the FIRST Spark call in the process, or the jar will not load.
# Missing connector -> "Failed to find data source: kafka"
