# T6.Checkpoint check (the slide's bash heredoc, as a module for Windows)
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from masar.config import LAKEHOUSE
from masar.spark import get_spark

s = get_spark("check")
path = str(LAKEHOUSE / "bronze" / "gps_stream")
(DeltaTable.forPath(s, path).history()
    .select("version", "operation", F.col("operationMetrics")["numOutputRows"].alias("rows"))
    .orderBy("version").show(truncate=False))
df = s.read.format("delta").load(path)
print("rows:", df.count(), " distinct (trip_id, event_ts):", df.select("trip_id", "event_ts").distinct().count())
