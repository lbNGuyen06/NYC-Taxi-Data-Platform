from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from nyc_taxi_data_platform.schemas import (
    TRIP_CANONICAL_TYPES,
    TRIP_EXPECTED_TYPES,
    TRIP_OPTIONAL_COLUMNS,
    TRIP_SOURCE_TO_CANONICAL,
    ZONE_SOURCE_TO_CANONICAL,
    ZONE_CANONICAL_TYPES,
)
from nyc_taxi_data_platform.validators import (
    validate_trips_schema,
)


SOURCE_RECORD_COLUMN = "_source_record"


def _add_missing_optional_columns(
    trips_df: DataFrame,
) -> DataFrame:
    result_df = trips_df

    for column_name in TRIP_OPTIONAL_COLUMNS:
        if column_name not in result_df.columns:
            result_df = result_df.withColumn(
                column_name,
                F.lit(None).cast(
                    TRIP_EXPECTED_TYPES[column_name]
                ),
            )

    return result_df


def _build_canonical_column(
    source_name: str,
    canonical_name: str,
) -> Column:
    expression = F.col(f"`{source_name}`")

    if source_name == "store_and_fwd_flag":
        expression = F.upper(F.trim(expression))

    return expression.try_cast(
        TRIP_CANONICAL_TYPES[canonical_name]
    ).alias(canonical_name)


def canonicalize_trips(
    trips_df: DataFrame,
) -> DataFrame:
    validate_trips_schema(trips_df)

    source_df = _add_missing_optional_columns(
        trips_df
    )

    source_record = F.struct(
        *[
            F.col(f"`{source_name}`").alias(
                source_name
            )
            for source_name
            in TRIP_SOURCE_TO_CANONICAL
        ]
    ).alias(SOURCE_RECORD_COLUMN)

    canonical_columns = [
        _build_canonical_column(
            source_name,
            canonical_name,
        )
        for source_name, canonical_name
        in TRIP_SOURCE_TO_CANONICAL.items()
    ]

    return source_df.select(
        source_record,
        *canonical_columns,
    )


def _build_canonical_zone_column(
    source_name: str,
    canonical_name: str,
) -> Column:
    expression = F.col(f"`{source_name}`")

    if source_name != "LocationID":
        expression = F.trim(expression)

    return expression.try_cast(
        ZONE_CANONICAL_TYPES[canonical_name]
    ).alias(canonical_name)


def canonicalize_zones(
    zones_df: DataFrame,
) -> DataFrame:
    canonical_columns = [
        _build_canonical_zone_column(
            source_name,
            canonical_name,
        )
        for source_name, canonical_name
        in ZONE_SOURCE_TO_CANONICAL.items()
    ]

    return zones_df.select(
        *canonical_columns,
    )
