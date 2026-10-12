import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
)

from nyc_taxi_data_platform.schemas import (
    ZONE_CANONICAL_TYPES,
)
from nyc_taxi_data_platform.validators import (
    validate_dim_zones,
)

ZONE_DIM_SCHEMA = StructType([
    StructField(
        column_name,
        data_type,
        True,
    )
    for column_name, data_type
    in ZONE_CANONICAL_TYPES.items()
])


def test_validate_dim_zones_accepts_valid_dimension(
    spark: SparkSession,
) -> None:
    zones_df = spark.createDataFrame(
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
                "Boro Zone",
            ),
        ],
        ZONE_DIM_SCHEMA,
    )

    result = validate_dim_zones(zones_df)

    assert result is None


def test_validate_dim_zones_empty_dataframe(spark: SparkSession) -> None:
    zones_df = spark.createDataFrame(
        [],
        ZONE_DIM_SCHEMA,
    )

    with pytest.raises(
        ValueError,
        match="DS004",
    ):
        validate_dim_zones(zones_df)


def test_validate_dim_zones_missing_zone_id(
    spark: SparkSession,
) -> None:
    schema_without_zone_id = StructType([
        StructField(
            column_name,
            data_type,
            True,
        )
        for column_name, data_type
        in ZONE_CANONICAL_TYPES.items()
        if column_name != "zone_id"
    ])
    zones_df = spark.createDataFrame(
        [],
        schema_without_zone_id,
    )

    with pytest.raises(
        ValueError,
        match="DS004",
    ):
        validate_dim_zones(zones_df)


def test_validate_dim_zones_duplicate_zone_ids(spark: SparkSession) -> None:
    zones_df = spark.createDataFrame(
        [
            (
                100,
                "Manhattan",
                "Garment District",
                "Yellow Zone",
            ),
            (
                100,  # Duplicate zone_id
                "Brooklyn",
                "Williamsburg",
                "Boro Zone",
            ),
        ],
        ZONE_DIM_SCHEMA,
    )

    with pytest.raises(
        ValueError,
        match="DS004",
    ):
        validate_dim_zones(zones_df)


def test_validate_dim_zones_null_zone_ids(spark: SparkSession) -> None:
    zones_df = spark.createDataFrame(
        [
            (
                None,
                "Manhattan",
                "Garment District",
                "Yellow Zone",
            ),
        ],
        ZONE_DIM_SCHEMA,
    )

    with pytest.raises(
        ValueError,
        match="DS004",
    ):
        validate_dim_zones(zones_df)


def test_validate_dim_zones_null_service_zone(spark: SparkSession) -> None:
    zones_df = spark.createDataFrame(
        [
            (
                100,
                "Manhattan",
                "Garment District",
                None,  # Null service_zone
            ),
        ],
        ZONE_DIM_SCHEMA,
    )

    result = validate_dim_zones(zones_df)

    assert result is None
