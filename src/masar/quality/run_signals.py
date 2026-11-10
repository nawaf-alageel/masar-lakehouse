# T7.S3 fast path: inject the faults + fire all three signals in ONE Spark session
# (instead of 4 separate `python -m` runs that each start Spark from scratch)
from masar.config import LAKEHOUSE, SILVER
from masar.quality import observe

HALF = str(LAKEHOUSE / "scratch" / "silver_half")
DRIFT = str(LAKEHOUSE / "scratch" / "silver_drift")

import masar.quality.inject_faults  # noqa: E402,F401  (runs the injection, prints "scratch copies ready")

for label, fn in [("freshness", lambda: observe.check_freshness(SILVER)),
                  ("volume",    lambda: observe.check_volume(HALF, 44371)),
                  ("drift",     lambda: observe.check_fare_drift(DRIFT, 60.2))]:
    print(f"\n--check {label}")
    try:
        fn()
    except RuntimeError as e:
        print(f"RuntimeError: {e}")
