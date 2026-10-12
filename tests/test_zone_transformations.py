from pyspark.sql import SparkSession

from nyc_taxi_data_platform.schemas import (
    ZONE_CANONICAL_TYPES,
    ZONE_SOURCE_SCHEMA,
)
from nyc_taxi_data_platform.transformations import (
    canonicalize_zones,
)


def test_canonicalize_zones_schema_and_column_names(
    spark: SparkSession,
) -> None:
    source_df = spark.createDataFrame(
        [
            (
                100,
                "Manhattan ",
                "Garment District ",
                "Yellow Zone ",
            ),
        ],
        ZONE_SOURCE_SCHEMA,
    )

    result_df = canonicalize_zones(source_df)

    expected_columns = [
        "zone_id",
        "borough",
        "zone_name",
        "service_zone",
    ]

    assert result_df.columns == expected_columns

    actual_types = {
        field.name: field.dataType
        for field in result_df.schema.fields
    }

    assert actual_types == ZONE_CANONICAL_TYPES


def test_canonicalize_zones_values(spark: SparkSession) -> None:
    source_df = spark.createDataFrame(
        [
            (
                100,
                " Manhattan ",
                " Garment District ",
                " Yellow Zone ",
            ),
        ],
        ZONE_SOURCE_SCHEMA,
    )

    result_df = canonicalize_zones(source_df)

    expected_values = [
        (100, "Manhattan", "Garment District", "Yellow Zone"),
    ]

    actual_values = [tuple(row) for row in result_df.collect()]

    assert actual_values == expected_values


def test_canonicalization_zones_preserves_record_count(spark: SparkSession) -> None:
    source_df = spark.createDataFrame(
        [
            (
                100,
                "Manhattan",
                "Garment District",
                "Yellow Zone",
            ),
            (
                101,
                "Brooklyn",
                "Williamsburg",
                "Green Zone",
            ),
        ],
        ZONE_SOURCE_SCHEMA,
    )

    result_df = canonicalize_zones(source_df)

    assert result_df.count() == source_df.count()


def test_canonicalization_zones_handles_none_values(spark: SparkSession) -> None:
    source_df = spark.createDataFrame(
        [
            (
                100,
                "Manhattan",
                "Garment District",
                None,
            ),
        ],
        ZONE_SOURCE_SCHEMA,
    )

    result_df = canonicalize_zones(source_df)

    result = result_df.first()

    assert result["zone_id"] == 100
    assert result["borough"] == "Manhattan"
    assert result["zone_name"] == "Garment District"
    assert result["service_zone"] is None
