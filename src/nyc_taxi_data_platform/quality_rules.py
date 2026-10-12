from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from functools import reduce
from operator import add, and_, or_

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


Rule = tuple[str, Column]

TEMPORAL_ERROR_RULE_IDS_COLUMN = (
    "_temporal_error_rule_ids"
)

SCALAR_ERROR_RULE_IDS_COLUMN = (
    "_scalar_error_rule_ids"
)

DQ025_REQUIRED_COLUMNS = (
    "vendor_id",
    "trip_distance_miles",
    "payment_type_id",
    "fare_amount",
    "extra_amount",
    "mta_tax_amount",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge_amount",
    "total_amount",
)
WARNING_RULE_IDS_COLUMN = "warning_rule_ids"

OPTIONAL_MONETARY_COLUMNS = (
    "congestion_surcharge_amount",
    "airport_fee_amount",
    "cbd_congestion_fee_amount",
)

TOTAL_COMPONENT_COLUMNS = (
    "fare_amount",
    "extra_amount",
    "mta_tax_amount",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge_amount",
    "congestion_surcharge_amount",
    "airport_fee_amount",
    "cbd_congestion_fee_amount",
)


def _matching_rule_ids(
    rules: Sequence[Rule],
) -> Column:
    nullable_rule_ids = F.array(
        *[
            F.when(
                condition,
                F.lit(rule_id),
            )
            for rule_id, condition in rules
        ]
    )

    return F.array_compact(nullable_rule_ids)


def apply_temporal_quality_rules(
    trips_df: DataFrame,
) -> DataFrame:
    temporal_error_rules = [
        (
            "DQ001",
            F.col("pickup_at").isNull(),
        ),
        (
            "DQ002",
            F.col("dropoff_at").isNull(),
        ),
        (
            "DQ003",
            F.col("dropoff_at")
            < F.col("pickup_at"),
        ),
        (
            "DQ004",
            F.col("dropoff_at")
            == F.col("pickup_at"),
        ),
    ]

    return trips_df.withColumn(
        TEMPORAL_ERROR_RULE_IDS_COLUMN,
        _matching_rule_ids(
            temporal_error_rules
        ),
    )


def _processing_month_bounds(
    processing_month: str,
) -> tuple[datetime, datetime]:
    try:
        month_start = datetime.strptime(
            processing_month,
            "%Y-%m",
        )
    except (TypeError, ValueError) as error:
        raise ValueError(
            "processing_month must use YYYY-MM format"
        ) from error

    if (
        month_start.strftime("%Y-%m")
        != processing_month
    ):
        raise ValueError(
            "processing_month must use YYYY-MM format"
        )

    if month_start.month == 12:
        next_month_start = datetime(
            month_start.year + 1,
            1,
            1,
        )
    else:
        next_month_start = datetime(
            month_start.year,
            month_start.month + 1,
            1,
        )

    return month_start, next_month_start


def _any_column_is_null(
    column_names: Sequence[str],
) -> Column:
    return reduce(
        or_,
        (
            F.col(column_name).isNull()
            for column_name in column_names
        ),
    )


def apply_scalar_error_quality_rules(
    trips_df: DataFrame,
    processing_month: str,
) -> DataFrame:
    (
        month_start,
        next_month_start,
    ) = _processing_month_bounds(
        processing_month
    )

    pickup_at = F.col("pickup_at")

    outside_processing_month = (
        pickup_at.isNotNull()
        & (
            (pickup_at < F.lit(month_start))
            | (
                pickup_at
                >= F.lit(next_month_start)
            )
        )
    )

    scalar_error_rules = [
        (
            "DQ005",
            outside_processing_month,
        ),
        (
            "DQ006",
            F.col("trip_distance_miles") <= 0,
        ),
        (
            "DQ007",
            F.col("fare_amount") < 0,
        ),
        (
            "DQ008",
            F.col("total_amount") < 0,
        ),
        (
            "DQ009",
            F.col("pickup_zone_id").isNull(),
        ),
        (
            "DQ010",
            F.col("dropoff_zone_id").isNull(),
        ),
        (
            "DQ014",
            F.col("vendor_id").isNotNull()
            & ~F.col("vendor_id").isin(
                1, 2, 6, 7
            ),
        ),
        (
            "DQ015",
            F.col("payment_type_id").isNotNull()
            & ~F.col("payment_type_id").isin(
                0, 1, 2, 3, 4, 5, 6
            ),
        ),
        (
            "DQ025",
            _any_column_is_null(
                DQ025_REQUIRED_COLUMNS
            ),
        ),
    ]

    return trips_df.withColumn(
        SCALAR_ERROR_RULE_IDS_COLUMN,
        _matching_rule_ids(
            scalar_error_rules
        ),
    )


def _all_columns_are_not_null(
    column_names: Sequence[str],
) -> Column:
    return reduce(
        and_,
        (
            F.col(column_name).isNotNull()
            for column_name in column_names
        ),
    )


def _sum_columns(
    column_names: Sequence[str],
) -> Column:
    return reduce(
        add,
        (
            F.col(column_name)
            for column_name in column_names
        ),
    )


def apply_warning_quality_rules(
    trips_df: DataFrame,
) -> DataFrame:
    rate_code_id = F.col("rate_code_id")
    store_and_fwd_flag = F.col(
        "store_and_fwd_flag"
    )
    passenger_count = F.col(
        "passenger_count"
    )

    duration_microseconds = F.timestamp_diff(
        "MICROSECOND",
        F.col("pickup_at"),
        F.col("dropoff_at"),
    )

    total_components = _sum_columns(
        TOTAL_COMPONENT_COLUMNS
    )

    all_components_present = (
        _all_columns_are_not_null(
            TOTAL_COMPONENT_COLUMNS
        )
    )

    amount_difference = F.abs(
        F.col("total_amount")
        - total_components
    )

    warning_rules = [
        (
            "DQ016",
            rate_code_id.isNotNull()
            & ~rate_code_id.isin(
                1, 2, 3, 4, 5, 6, 99
            ),
        ),
        (
            "DQ017",
            store_and_fwd_flag.isNotNull()
            & ~store_and_fwd_flag.isin(
                "Y", "N"
            ),
        ),
        (
            "DQ018",
            passenger_count.isNull(),
        ),
        (
            "DQ019",
            passenger_count.isNotNull()
            & (
                (passenger_count < 0)
                | (passenger_count > 8)
            ),
        ),
        (
            "DQ020",
            _any_column_is_null(
                OPTIONAL_MONETARY_COLUMNS
            ),
        ),
        (
            "DQ021",
            F.col("trip_distance_miles")
            > Decimal("200.00"),
        ),
        (
            "DQ022",
            duration_microseconds
            > 86_400_000_000,
        ),
        (
            "DQ023",
            F.col("total_amount")
            > Decimal("1000.00"),
        ),
        (
            "DQ024",
            F.col("total_amount").isNotNull()
            & all_components_present
            & (
                amount_difference
                > F.lit(Decimal("0.01"))
            ),
        ),
    ]

    return trips_df.withColumn(
        WARNING_RULE_IDS_COLUMN,
        _matching_rule_ids(warning_rules),
    )
