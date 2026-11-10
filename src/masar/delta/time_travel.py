import json, sys
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from masar.config import SILVER
from masar.spark import get_spark
spark = get_spark("time-travel")


def history() -> None:  # the audit trail
    (DeltaTable.forPath(spark, SILVER).history()  # one row per commit in _delta_log
        .select("version", "timestamp", "operation",  # version, when, and what (CREATE, MERGE, DELETE...)
                F.col("operationMetrics")["numTargetRowsUpdated"].alias("updated"),  # MERGE's updated-row count
                F.col("operationMetrics")["numDeletedRows"].alias("deleted"))
        .orderBy("version").show(truncate=False))  # oldest first, in full


def rider_by_version(rider_id: str) -> None:  # time travel
    versions = [r.version for r in DeltaTable.forPath(spark, SILVER).history()
                .select("version").orderBy("version").collect()]  # e.g. [0, 1, 2, 3, 4]
    for v in versions:
        n = (spark.read.format("delta").option("versionAsOf", v).load(SILVER)
             .where(F.col("rider_id") == rider_id).count())  # that rider's trips in that version
        print(f"v{v}: {rider_id} rows = {n}")


if __name__ == "__main__":
    rid = json.load(open("data/fixtures/erasure_request.json"))["rider_id"]
    {"history": history, "rider": lambda: rider_by_version(rid)}[sys.argv[1]]()

# history -> v0 CREATE, v1-v2 ADD CONSTRAINT, v3 MERGE (5), v4 DELETE (7)  |  rider -> v0-v3: 7 rows, v4: 0
