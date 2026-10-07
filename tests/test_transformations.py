from datetime import datetime
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType

from nyc_taxi_data_platform.schemas import (
    TRIP_CANONICAL_TYPES,
    TRIP_EXPECTED_TYPES,
    TRIP_SOURCE_TO_CANONICAL,
)
from nyc_taxi_data_platform.transformations import (
    SOURCE_RECORD_COLUMN,
    canonicalize_trips,
)


def build_source_schema(
    excluded_columns: set[str] | None = None,
) -> StructType:
    excluded_columns = excluded_columns or set()

    return StructType([
        StructField(
            column_name,
            data_type,
            True,
        )
        for column_name, data_type
        in TRIP_EXPECTED_TYPES.items()
        if column_name not in excluded_columns
    ])


def valid_source_record() -> dict:
    return {
        "VendorID": 1,
        "tpep_pickup_datetime": datetime(
            2025, 1, 1, 8, 0
        ),
        "tpep_dropoff_datetime": datetime(
            2025, 1, 1, 8, 10
        ),
        "passenger_count": 2,
        "trip_distance": 1.25,
        "RatecodeID": 1,
        "store_and_fwd_flag": " y ",
        "PULocationID": 100,
        "DOLocationID": 200,
        "payment_type": 1,
        "fare_amount": 10.0,
        "extra": 1.0,
        "mta_tax": 0.5,
        "tip_amount": 2.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 1.0,
        "total_amount": 14.5,
        "congestion_surcharge": 0.0,
        "Airport_fee": 0.0,
        "cbd_congestion_fee": 0.0,
    }


def test_canonicalize_trips_creates_expected_schema(
    spark: SparkSession,
) -> None:
    source_df = spark.createDataFrame(
        [valid_source_record()],
        build_source_schema(),
    )

    result_df = canonicalize_trips(source_df)

    assert result_df.columns == [
        SOURCE_RECORD_COLUMN,
        *TRIP_SOURCE_TO_CANONICAL.values(),
    ]

    result_types = {
        field.name: field.dataType
        for field in result_df.schema.fields
    }

    for column_name, expected_type in (
        TRIP_CANONICAL_TYPES.items()
    ):
        assert (
            result_types[column_name]
            == expected_type
        )

    result = result_df.first()

    assert result.vendor_id == 1
    assert result.store_and_fwd_flag == "Y"
    assert result.fare_amount == Decimal("10.00")
    assert (
        result.trip_distance_miles
        == Decimal("1.25")
    )


def test_canonicalize_trips_preserves_source_record(
    spark: SparkSession,
) -> None:
    source_df = spark.createDataFrame(
        [valid_source_record()],
        build_source_schema(),
    )

    result = canonicalize_trips(
        source_df
    ).first()

    source_record = result[SOURCE_RECORD_COLUMN]

    assert source_record["VendorID"] == 1
    assert (
        source_record["store_and_fwd_flag"]
        == " y "
    )


def test_missing_optional_column_becomes_typed_null(
    spark: SparkSession,
) -> None:
    record = valid_source_record()
    record.pop("Airport_fee")

    source_df = spark.createDataFrame(
        [record],
        build_source_schema(
            excluded_columns={"Airport_fee"}
        ),
    )

    result_df = canonicalize_trips(source_df)
    result = result_df.first()

    assert result.airport_fee_amount is None
    assert (
        result_df.schema[
            "airport_fee_amount"
        ].dataType
        == TRIP_CANONICAL_TYPES[
            "airport_fee_amount"
        ]
    )


def test_failed_cast_returns_null_and_preserves_source(
    spark: SparkSession,
) -> None:
    record = valid_source_record()
    record["passenger_count"] = 2_147_483_648

    source_df = spark.createDataFrame(
        [record],
        build_source_schema(),
    )

    result = canonicalize_trips(
        source_df
    ).first()

    assert result.passenger_count is None
    assert (
        result[SOURCE_RECORD_COLUMN][
            "passenger_count"
        ]
        == 2_147_483_648
    )


def test_canonicalization_preserves_record_count(
    spark: SparkSession,
) -> None:
    first_record = valid_source_record()

    second_record = valid_source_record()
    second_record["VendorID"] = 2
    second_record["PULocationID"] = 150

    source_df = spark.createDataFrame(
        [first_record, second_record],
        build_source_schema(),
    )

    result_df = canonicalize_trips(source_df)

    assert result_df.count() == source_df.count()
