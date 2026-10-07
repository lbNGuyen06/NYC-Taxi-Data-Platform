from pyspark.sql.types import DecimalType

from nyc_taxi_data_platform.schemas import (
    TRIP_CANONICAL_TYPES,
    TRIP_EXPECTED_TYPES,
    TRIP_OPTIONAL_COLUMNS,
    TRIP_REQUIRED_COLUMNS,
    TRIP_SOURCE_TO_CANONICAL,
)


def test_source_columns_are_classified_once() -> None:
    assert TRIP_REQUIRED_COLUMNS.isdisjoint(
        TRIP_OPTIONAL_COLUMNS
    )

    assert (
        TRIP_REQUIRED_COLUMNS | TRIP_OPTIONAL_COLUMNS
        == set(TRIP_EXPECTED_TYPES)
    )


def test_every_source_column_has_canonical_mapping() -> None:
    assert (
        set(TRIP_SOURCE_TO_CANONICAL)
        == set(TRIP_EXPECTED_TYPES)
    )


def test_every_canonical_column_has_type() -> None:
    assert (
        set(TRIP_SOURCE_TO_CANONICAL.values())
        == set(TRIP_CANONICAL_TYPES)
    )


def test_canonical_names_are_unique() -> None:
    canonical_names = list(
        TRIP_SOURCE_TO_CANONICAL.values()
    )

    assert len(canonical_names) == len(
        set(canonical_names)
    )


def test_money_columns_use_fixed_precision_decimal() -> None:
    money_columns = {
        "fare_amount",
        "extra_amount",
        "mta_tax_amount",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge_amount",
        "total_amount",
        "congestion_surcharge_amount",
        "airport_fee_amount",
        "cbd_congestion_fee_amount",
    }

    for column_name in money_columns:
        assert (
            TRIP_CANONICAL_TYPES[column_name]
            == DecimalType(12, 2)
        )
