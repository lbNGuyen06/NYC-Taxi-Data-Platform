import logging

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DataType,
    StringType,
    StructField,
    StructType,
)

from nyc_taxi_data_platform.schemas import TRIP_EXPECTED_TYPES
from nyc_taxi_data_platform.validators import validate_trips_schema


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession
        .builder
        .master("local[1]")
        .appName("test-trip-schema-validator")
        .config("spark.sql.session.timezone", "America/New_York")
        .config("spark.sql.timestampType", "TIMESTAMP_NTZ")
        .getOrCreate()
    )
    yield spark_session

    spark_session.stop()


def build_trip_schema(
    excluded_columns: set[str] | None = None,
    type_overrides: dict[str, DataType] | None = None,
    extra_fields: list[StructField] | None = None
) -> StructType:
    excluded_columns = excluded_columns or set()
    type_overrides = type_overrides or {}
    extra_fields = extra_fields or []
    fields = [
        StructField(
            column_name,
            type_overrides.get(column_name, expected_type),
            True,
        )
        for column_name, expected_type in TRIP_EXPECTED_TYPES.items()
        if column_name not in excluded_columns
    ]
    fields.extend(extra_fields)

    return StructType(fields)


def test_vali_schema_passes(spark: SparkSession) -> None:
    schema = build_trip_schema()
    trips_df = spark.createDataFrame([], schema)
    result = validate_trips_schema(trips_df)
    assert result is None  # No exception should be raised


def test_missing_required_column_raises_error(
    spark: SparkSession,
) -> None:
    schema = build_trip_schema(
        excluded_columns={"fare_amount"}
    )
    trips_df = spark.createDataFrame([], schema)

    with pytest.raises(ValueError, match="fare_amount"):
        validate_trips_schema(trips_df)


def test_missing_optional_columns_warning(
    spark: SparkSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    schema = build_trip_schema(
        excluded_columns={"Airport_fee"}
    )
    trips_df = spark.createDataFrame([], schema)
    with caplog.at_level(
        logging.WARNING,
        logger="nyc_taxi_data_platform.validators",
    ):
        result = validate_trips_schema(trips_df)

    assert result is None
    assert "Missing optional trip columns" in caplog.text
    assert "Airport_fee" in caplog.text


def test_incompatible_type_raises_error(
    spark: SparkSession,
) -> None:
    schema = build_trip_schema(
        type_overrides={"fare_amount": StringType()}
    )
    trips_df = spark.createDataFrame([], schema)

    with pytest.raises(TypeError) as error:
        validate_trips_schema(trips_df)

    message = str(error.value)

    assert "fare_amount" in message
    assert "double" in message
    assert "string" in message


def test_unexpected_columns_logs_warning(spark: SparkSession, caplog: pytest.LogCaptureFixture,) -> None:
    schema = build_trip_schema(
        extra_fields=[
            StructField(
                "new_tlc_column",
                StringType(),
                True,
            )
        ]
    )
    trips_df = spark.createDataFrame([], schema)

    with caplog.at_level(
        logging.WARNING,
        logger="nyc_taxi_data_platform.validators",
    ):
        result = validate_trips_schema(trips_df)

    assert result is None
    assert "Unexpected trip columns" in caplog.text
    assert "new_tlc_column" in caplog.text
