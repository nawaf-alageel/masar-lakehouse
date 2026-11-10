# T8 verify: gold matches silver, no duplicate grain (the slide's bash heredoc, as a module for Windows)
from pyspark.sql import functions as F
from masar.config import GOLD, SILVER
from masar.spark import get_spark

s = get_spark("verify")
silver = s.read.format("delta").load(SILVER).count()
d = s.read.format("delta").load(f"{GOLD}/zone_hourly_demand")
f = s.read.format("delta").load(f"{GOLD}/trip_features")
print("silver rows        :", silver)
print("demand rows        :", d.count())
print("demand sum(trips)  :", d.agg(F.sum("trips")).first()[0])
print("demand grain dups  :", d.groupBy("city", "date", "hour").count().filter("count > 1").count())
print("features rows      :", f.count())
print("features dup trips :", f.groupBy("trip_id").count().filter("count > 1").count())
