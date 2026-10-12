import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from nyc_taxi_data_platform.schemas import (
    TRIP_EXPECTED_TYPES,
    TRIP_OPTIONAL_COLUMNS,
    TRIP_REQUIRED_COLUMNS,
)


logger = logging.getLogger(__name__)


def validate_trips_schema(trips_df: DataFrame) -> None:
    actual_types = {
        field.name: field.dataType
        for field in trips_df.schema.fields
    }

    actual_columns = set(actual_types)
    missing_required = TRIP_REQUIRED_COLUMNS - actual_columns
    missing_optional = TRIP_OPTIONAL_COLUMNS - actual_columns
    expected_columns = TRIP_REQUIRED_COLUMNS | TRIP_OPTIONAL_COLUMNS
    unexpected_columns = actual_columns - expected_columns

    if missing_required:
        raise ValueError(
            f"Missing required trip columns: {sorted(missing_required)}"
        )

    if missing_optional:
        logger.warning(
            "Missing optional trip columns: %s",
            sorted(missing_optional),
        )

    if unexpected_columns:
        logger.warning(
            "Unexpected trip columns: %s",
            sorted(unexpected_columns),
        )

    type_mismatches = {}
    for column_name, expected_type in TRIP_EXPECTED_TYPES.items():
        actual_type = actual_types.get(column_name)
        if actual_type is None:
            continue
        if actual_type != expected_type:
            type_mismatches[column_name] = {
                "expected": expected_type.simpleString(),
                "actual": actual_type.simpleString(),
            }
    if type_mismatches:
        raise TypeError(
            f"Trip column type mismatches: {type_mismatches}"
        )


def validate_dim_zones(zones_df: DataFrame) -> None:
    if "zone_id" not in zones_df.columns:
        raise ValueError(
            "DS004: dim_zone must include zone_id"
        )

    summary = zones_df.agg(
        F.count("*").alias("row_count"),
        F.count_distinct("zone_id").alias(
            "distinct_zone_id_count"
        ),
    ).first()

    if summary.row_count == 0:
        raise ValueError(
            "DS004: dim_zone must not be empty"
        )

    if (
        summary.row_count
        != summary.distinct_zone_id_count
    ):
        raise ValueError(
            "DS004: zone_id must be non-null and unique"
        )
