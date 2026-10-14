import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from nyc_taxi_data_platform.quality_rules import (
    ZONE_REFERENCE_ERROR_RULE_IDS_COLUMN,
    apply_zone_reference_quality_rules,
)


TRIP_ZONE_SCHEMA = StructType([
    StructField("record_id", StringType(), False),
    StructField("pickup_zone_id", IntegerType(), True),
    StructField("dropoff_zone_id", IntegerType(), True),
])

ZONE_ID_SCHEMA = StructType([
    StructField("zone_id", IntegerType(), False),
])


@pytest.mark.parametrize(
    (
        "pickup_zone_id",
        "dropoff_zone_id",
        "expected_rule_ids",
    ),
    [
        (100, 200, []),
        (999, 200, ["DQ011"]),
        (100, 999, ["DQ012"]),
        (998, 999, ["DQ011", "DQ012"]),
        (None, 200, []),
        (100, None, []),
    ],
)
def test_apply_zone_reference_quality_rules(
    spark: SparkSession,
    pickup_zone_id: int | None,
    dropoff_zone_id: int | None,
    expected_rule_ids: list[str],
) -> None:
    trips_df = spark.createDataFrame(
        [
            (
                "trip-1",
                pickup_zone_id,
                dropoff_zone_id,
            )
        ],
        schema=TRIP_ZONE_SCHEMA,
    )
    zones_df = spark.createDataFrame(
        [(100,), (200,)],
        schema=ZONE_ID_SCHEMA,
    )

    result_df = apply_zone_reference_quality_rules(
        trips_df,
        zones_df,
    )
    result = result_df.first()

    assert (
        result[ZONE_REFERENCE_ERROR_RULE_IDS_COLUMN]
        == expected_rule_ids
    )


def test_zone_reference_rules_preserve_records(
    spark: SparkSession,
) -> None:
    trips_df = spark.createDataFrame(
        [
            ("trip-1", 100, 200),
            ("trip-2", 999, 200),
            ("trip-3", 100, None),
        ],
        schema=TRIP_ZONE_SCHEMA,
    )
    zones_df = spark.createDataFrame(
        [(100,), (200,)],
        schema=ZONE_ID_SCHEMA,
    )

    result_df = apply_zone_reference_quality_rules(
        trips_df,
        zones_df,
    )

    assert result_df.count() == trips_df.count()
    assert result_df.columns == [
        *trips_df.columns,
        ZONE_REFERENCE_ERROR_RULE_IDS_COLUMN,
    ]
