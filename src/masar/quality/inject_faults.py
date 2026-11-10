# T7 Checkpoint "Signals Fire": make scratch copies of silver with injected faults
# (the slide's bash heredoc, as a module for Windows)
from pyspark.sql import functions as F
from masar.config import LAKEHOUSE, SILVER
from masar.spark import get_spark

s = get_spark("inject")
df = s.read.format("delta").load(SILVER)
df.limit(df.count() // 2).write.format("delta").mode("overwrite").save(str(LAKEHOUSE / "scratch" / "silver_half"))
df.withColumn("fare_sar", F.col("fare_sar") * 1.3).write.format("delta").mode("overwrite").save(str(LAKEHOUSE / "scratch" / "silver_drift"))
print("scratch copies ready")
