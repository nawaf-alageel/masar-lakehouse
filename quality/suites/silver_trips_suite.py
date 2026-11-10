# quality/suites/silver_trips_suite.py  (slide 99: the data contract as written on the slide)
# The runnable version used in class is src/masar/quality/contract.py (same expectations).
def build_suite(context) -> None:
    suite = context.add_or_update_expectation_suite("silver_trips.warning")
    validator = context.get_validator(
        batch_request=context.get_datasource("masar_lakehouse")
            .get_asset("silver_trips").build_batch_request(),
        expectation_suite=suite)

    # Schema / integrity: fail-fast candidates
    validator.expect_column_values_to_not_be_null("trip_id")
    validator.expect_column_values_to_be_unique("trip_id")
    validator.expect_column_values_to_be_of_type("fare_sar", "DoubleType")

    # Range / set: mixed fail/quarantine
    validator.expect_column_values_to_be_between("fare_sar", min_value=0, strict_min=True)
    validator.expect_column_values_to_be_in_set(
        "city", ["Riyadh", "Jeddah", "Dammam", "Mecca", "Medina"])
    validator.expect_column_values_to_be_between("duration_min", min_value=0, max_value=600)

    # Distribution: scheduled drift guard
    validator.expect_column_mean_to_be_between("fare_sar", min_value=15, max_value=120)
    validator.save_expectation_suite(discard_failed_expectations=False)
