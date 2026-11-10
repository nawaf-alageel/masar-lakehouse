from pyspark.sql import functions as F
from masar.config import LAKEHOUSE, SILVER
from masar.spark import get_spark

spark = get_spark("evolve")
DEMO = str(LAKEHOUSE / "scratch" / "evolve_demo")

base = spark.read.format("delta").load(SILVER).limit(5)
base.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(DEMO)  # reset schema too, so re-runs start clean
new = base.withColumn("promo_code", F.lit("RAMADAN10"))    # a genuinely new column

try:
    new.write.format("delta").mode("append").save(DEMO)
    print("UNEXPECTED")
except Exception as e:
    print("REJECTED:", type(e).__name__, str(e)[:200])     # schema mismatch

new.write.format("delta").mode("append").option("mergeSchema", "true").save(DEMO)
spark.read.format("delta").load(DEMO).select("trip_id", "promo_code").show()
# 10 rows: 5 with RAMADAN10, 5 null (the rows from before the column existed)
