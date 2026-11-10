# T7.S4 prep: copy bronze GPS pings to a scratch table so the retention demo never touches real bronze
# (the slide's bash heredoc, as a module for Windows)
from pyspark.sql import functions as F
from masar.config import LAKEHOUSE
from masar.spark import get_spark

s = get_spark("prep")
df = s.read.format("delta").load(str(LAKEHOUSE / "bronze" / "gps_pings"))
df.write.format("delta").mode("overwrite").save(str(LAKEHOUSE / "scratch" / "gps_retention"))
df.agg(F.min("event_ts").alias("min"), F.max("event_ts").alias("max")).show(truncate=False)
