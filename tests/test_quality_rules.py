from datetime import datetime, timedelta
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    TimestampNTZType,
)

from nyc_taxi_data_platform.quality_rules import (
    DQ025_REQUIRED_COLUMNS,
    OPTIONAL_MONETARY_COLUMNS,
    SCALAR_ERROR_RULE_IDS_COLUMN,
    TEMPORAL_ERROR_RULE_IDS_COLUMN,
    WARNING_RULE_IDS_COLUMN,
    apply_scalar_error_quality_rules,
    apply_temporal_quality_rules,
    apply_warning_quality_rules,
)
from nyc_taxi_data_platform.schemas import (
    TRIP_CANONICAL_TYPES,
)


TEMPORAL_SCHEMA = StructType([
    StructField(
        "pickup_at",
        TimestampNTZType(),
        True,
    ),
    StructField(
        "dropoff_at",
        TimestampNTZType(),
        True,
    ),
])

CANONICAL_SCHEMA = StructType([
    StructField(
        column_name,
        data_type,
        True,
    )
    for column_name, data_type
    in TRIP_CANONICAL_TYPES.items()
])


def valid_canonical_record() -> dict:
    return {
        "vendor_id": 1,
        "pickup_at": datetime(2025, 1, 1, 8, 0),
        "dropoff_at": datetime(2025, 1, 1, 8, 10),
        "passenger_count": 1,
        "trip_distance_miles": Decimal("1.25"),
        "rate_code_id": 1,
        "store_and_fwd_flag": "N",
        "pickup_zone_id": 100,
        "dropoff_zone_id": 200,
        "payment_type_id": 1,
        "fare_amount": Decimal("10.00"),
        "extra_amount": Decimal("1.00"),
        "mta_tax_amount": Decimal("0.50"),
        "tip_amount": Decimal("2.00"),
        "tolls_amount": Decimal("0.00"),
        "improvement_surcharge_amount": Decimal("1.00"),
        "total_amount": Decimal("14.50"),
        "congestion_surcharge_amount": Decimal("0.00"),
        "airport_fee_amount": Decimal("0.00"),
        "cbd_congestion_fee_amount": Decimal("0.00"),
    }


@pytest.mark.parametrize(
    (
        "pickup_at",
        "dropoff_at",
        "expected_rule_ids",
    ),
    [
        (
            datetime(2025, 1, 1, 8, 0),
            datetime(2025, 1, 1, 8, 10),
            [],
        ),
        (
            None,
            datetime(2025, 1, 1, 8, 10),
            ["DQ001"],
        ),
        (
            datetime(2025, 1, 1, 8, 0),
            None,
            ["DQ002"],
        ),
        (
            datetime(2025, 1, 1, 8, 10),
            datetime(2025, 1, 1, 8, 0),
            ["DQ003"],
        ),
        (
            datetime(2025, 1, 1, 8, 0),
            datetime(2025, 1, 1, 8, 0),
            ["DQ004"],
        ),
        (
            None,
            None,
            ["DQ001", "DQ002"],
        ),
    ],
)
def test_temporal_quality_rules(
    spark: SparkSession,
    pickup_at,
    dropoff_at,
    expected_rule_ids: list[str],
) -> None:
    trips_df = spark.createDataFrame(
        [(pickup_at, dropoff_at)],
        TEMPORAL_SCHEMA,
    )

    result = apply_temporal_quality_rules(
        trips_df
    ).first()

    assert (
        result[TEMPORAL_ERROR_RULE_IDS_COLUMN]
        == expected_rule_ids
    )


def test_temporal_rules_preserve_record_count(
    spark: SparkSession,
) -> None:
    trips_df = spark.createDataFrame(
        [
            (
                datetime(2025, 1, 1, 8, 0),
                datetime(2025, 1, 1, 8, 10),
            ),
            (
                None,
                datetime(2025, 1, 1, 9, 0),
            ),
        ],
        TEMPORAL_SCHEMA,
    )

    result_df = apply_temporal_quality_rules(
        trips_df
    )

    assert result_df.count() == trips_df.count()


@pytest.mark.parametrize(
    ("updates", "expected_rule_ids"),
    [
        ({}, []),
        (
            {
                "pickup_at": datetime(
                    2024, 12, 31, 23, 59
                )
            },
            ["DQ005"],
        ),
        (
            {
                "trip_distance_miles":
                    Decimal("0.00")
            },
            ["DQ006"],
        ),
        (
            {
                "fare_amount":
                Decimal("-0.01")
            },
            ["DQ007"]
        ),
        (
            {
                "total_amount":
                Decimal("-0.01")
            },
            ["DQ008"]
        ),
        (
            {
                "pickup_zone_id": None,
            },
            ["DQ009"],
        ),
        (
            {
                "dropoff_zone_id": None,
            },
            ["DQ010"],
        ),
        (
            {
                "vendor_id": 3,
            },
            ["DQ014"],
        ),
        (
            {
                "payment_type_id": 7,
            },
            ["DQ015"],
        ),
        (
            {
                "extra_amount": None,
            },
            ["DQ025"],
        ),
        (
            {
                "pickup_at": datetime(
                    2025, 2, 1, 0, 0
                ),
            },
            ["DQ005"],
        ),
        (
            {
                "pickup_at": datetime(
                    2024, 12, 31, 23, 59
                ),
                "trip_distance_miles":
                    Decimal("0.00"),
                "fare_amount": Decimal("-0.01"),
                "total_amount": Decimal("-0.01"),
                "pickup_zone_id": None,
                "dropoff_zone_id": None,
                "vendor_id": 3,
                "payment_type_id": 7,
                "extra_amount": None,
            },
            [
                "DQ005",
                "DQ006",
                "DQ007",
                "DQ008",
                "DQ009",
                "DQ010",
                "DQ014",
                "DQ015",
                "DQ025",
            ],
        ),
    ],
)
def test_scalar_error_quality_rules(
    spark: SparkSession,
    updates: dict,
    expected_rule_ids: list[str],
) -> None:
    record = valid_canonical_record()
    record.update(updates)

    trips_df = spark.createDataFrame(
        [record],
        CANONICAL_SCHEMA,
    )

    result = apply_scalar_error_quality_rules(
        trips_df,
        processing_month="2025-01",
    ).first()

    assert (
        result[SCALAR_ERROR_RULE_IDS_COLUMN]
        == expected_rule_ids
    )


@pytest.mark.parametrize(
    "invalid_month",
    [
        "2025-1",
        "2025-13",
        "2025/01",
        "January-2025",
        "",
    ],
)
def test_invalid_processing_month_fails(
    spark: SparkSession,
    invalid_month: str,
) -> None:
    trips_df = spark.createDataFrame(
        [valid_canonical_record()],
        CANONICAL_SCHEMA,
    )

    with pytest.raises(
        ValueError,
        match="YYYY-MM",
    ):
        apply_scalar_error_quality_rules(
            trips_df,
            invalid_month,
        )


@pytest.mark.parametrize(
    ("pickup_at", "expected_rule_ids"),
    [
        (
            datetime(2025, 12, 31, 23, 59, 59),
            [],
        ),
        (
            datetime(2026, 1, 1, 0, 0),
            ["DQ005"],
        ),
    ],
)
def test_december_processing_month_boundary(
    spark: SparkSession,
    pickup_at: datetime,
    expected_rule_ids: list[str],
) -> None:
    record = valid_canonical_record()
    record["pickup_at"] = pickup_at

    trips_df = spark.createDataFrame(
        [record],
        CANONICAL_SCHEMA,
    )

    result = apply_scalar_error_quality_rules(
        trips_df,
        processing_month="2025-12",
    ).first()

    assert (
        result[SCALAR_ERROR_RULE_IDS_COLUMN]
        == expected_rule_ids
    )


@pytest.mark.parametrize(
    "column_name",
    DQ025_REQUIRED_COLUMNS,
)
def test_each_required_null_matches_dq025(
    spark: SparkSession,
    column_name: str,
) -> None:
    record = valid_canonical_record()
    record[column_name] = None

    trips_df = spark.createDataFrame(
        [record],
        CANONICAL_SCHEMA,
    )

    result = apply_scalar_error_quality_rules(
        trips_df,
        processing_month="2025-01",
    ).first()

    assert (
        "DQ025"
        in result[SCALAR_ERROR_RULE_IDS_COLUMN]
    )


@pytest.mark.parametrize(
    ("updates", "expected_rule_ids"),
    [
        ({}, []),
        (
            {"rate_code_id": 98},
            ["DQ016"],
        ),
        (
            {"rate_code_id": None},
            [],
        ),
        (
            {"store_and_fwd_flag": "X"},
            ["DQ017"],
        ),
        (
            {"store_and_fwd_flag": None},
            [],
        ),
        (
            {"passenger_count": None},
            ["DQ018"],
        ),
        (
            {"passenger_count": -1},
            ["DQ019"],
        ),
        (
            {"passenger_count": 9},
            ["DQ019"],
        ),
        (
            {"airport_fee_amount": None},
            ["DQ020"],
        ),
        (
            {
                "trip_distance_miles":
                    Decimal("200.01")
            },
            ["DQ021"],
        ),
        (
            {
                "total_amount":
                    Decimal("1000.01")
            },
            ["DQ023", "DQ024"],
        ),
    ],
)
def test_warning_quality_rules(
    spark: SparkSession,
    updates: dict,
    expected_rule_ids: list[str],
) -> None:
    record = valid_canonical_record()
    record.update(updates)

    trips_df = spark.createDataFrame(
        [record],
        CANONICAL_SCHEMA,
    )

    result = apply_warning_quality_rules(
        trips_df
    ).first()

    assert (
        result[WARNING_RULE_IDS_COLUMN]
        == expected_rule_ids
    )


@pytest.mark.parametrize(
    "column_name",
    OPTIONAL_MONETARY_COLUMNS,
)
def test_each_optional_monetary_null_matches_dq020(
    spark: SparkSession,
    column_name: str,
) -> None:
    record = valid_canonical_record()
    record[column_name] = None

    trips_df = spark.createDataFrame(
        [record],
        CANONICAL_SCHEMA,
    )

    result = apply_warning_quality_rules(
        trips_df
    ).first()

    assert result[WARNING_RULE_IDS_COLUMN] == [
        "DQ020"
    ]


def test_distance_at_threshold_has_no_warning(
    spark: SparkSession,
) -> None:
    record = valid_canonical_record()
    record["trip_distance_miles"] = Decimal(
        "200.00"
    )

    trips_df = spark.createDataFrame(
        [record],
        CANONICAL_SCHEMA,
    )
    result = apply_warning_quality_rules(
        trips_df
    ).first()

    assert result[WARNING_RULE_IDS_COLUMN] == []


@pytest.mark.parametrize(
    ("duration", "expected_rule_ids"),
    [
        (
            timedelta(hours=24),
            [],
        ),
        (
            timedelta(
                hours=24,
                microseconds=1,
            ),
            ["DQ022"],
        ),
    ],
)
def test_duration_warning_boundary(
    spark: SparkSession,
    duration: timedelta,
    expected_rule_ids: list[str],
) -> None:
    record = valid_canonical_record()
    record["dropoff_at"] = (
        record["pickup_at"] + duration
    )

    trips_df = spark.createDataFrame(
        [record],
        CANONICAL_SCHEMA,
    )

    result = apply_warning_quality_rules(
        trips_df
    ).first()

    assert (
        result[WARNING_RULE_IDS_COLUMN]
        == expected_rule_ids
    )


@pytest.mark.parametrize(
    ("total_amount", "expected"),
    [
        (Decimal("14.48"), ["DQ024"]),
        (Decimal("14.49"), []),
        (Decimal("14.50"), []),
        (Decimal("14.51"), []),
        (Decimal("14.52"), ["DQ024"]),
    ],
)
def test_total_amount_mismatch_warning(
    spark: SparkSession,
    total_amount: Decimal,
    expected: list[str],
) -> None:
    record = valid_canonical_record()
    record["total_amount"] = total_amount

    trips_df = spark.createDataFrame(
        [record],
        CANONICAL_SCHEMA,
    )

    result = apply_warning_quality_rules(
        trips_df
    ).first()

    assert result[WARNING_RULE_IDS_COLUMN] == expected


def test_null_component_no_mismatch_warning(
    spark: SparkSession,
) -> None:
    record = valid_canonical_record()
    # Gán component thành None, tổng các thành phần khác vẫn không đổi
    record["airport_fee_amount"] = None

    trips_df = spark.createDataFrame(
        [record],
        CANONICAL_SCHEMA,
    )

    result = apply_warning_quality_rules(
        trips_df
    ).first()

    # Missing optional component triggers DQ020, not DQ024.
    assert result[WARNING_RULE_IDS_COLUMN] == [
        "DQ020"
    ]


def test_multiple_warnings(
    spark: SparkSession,
) -> None:
    record = valid_canonical_record()
    record.update({
        "rate_code_id": 98,
        "passenger_count": 9,
        "trip_distance_miles": Decimal("201.00"),
    })

    trips_df = spark.createDataFrame(
        [record],
        CANONICAL_SCHEMA,
    )

    result = apply_warning_quality_rules(
        trips_df
    ).first()

    assert result[WARNING_RULE_IDS_COLUMN] == [
        "DQ016",
        "DQ019",
        "DQ021",
    ]
