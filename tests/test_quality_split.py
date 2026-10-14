import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)

from nyc_taxi_data_platform.quality import (
    split_trips_by_quality,
)
from nyc_taxi_data_platform.quality_rules import (
    ERROR_RULE_IDS_COLUMN,
    WARNING_RULE_IDS_COLUMN,
)


RULE_IDS_TYPE = ArrayType(
    StringType(),
    containsNull=False,
)

EVALUATED_TRIP_SCHEMA = StructType([
    StructField(
        "record_id",
        StringType(),
        False,
    ),
    StructField(
        ERROR_RULE_IDS_COLUMN,
        RULE_IDS_TYPE,
        False,
    ),
    StructField(
        WARNING_RULE_IDS_COLUMN,
        RULE_IDS_TYPE,
        False,
    ),
])


def _create_evaluated_trips(
    spark: SparkSession,
    records: list[tuple[str, list[str], list[str]]],
) -> DataFrame:
    return spark.createDataFrame(
        records,
        EVALUATED_TRIP_SCHEMA,
    )


def test_valid_trip_has_non_null_empty_rule_arrays(
    spark: SparkSession,
) -> None:
    trips_df = _create_evaluated_trips(
        spark,
        [("valid", [], [])],
    )

    split = split_trips_by_quality(trips_df)
    accepted_trip = split.accepted.first()

    assert split.accepted.count() == 1
    assert split.quarantine.count() == 0
    assert accepted_trip[ERROR_RULE_IDS_COLUMN] == []
    assert accepted_trip[WARNING_RULE_IDS_COLUMN] == []


def test_warning_only_trip_is_accepted(
    spark: SparkSession,
) -> None:
    trips_df = _create_evaluated_trips(
        spark,
        [("warning", [], ["DQ021"])],
    )

    split = split_trips_by_quality(trips_df)

    assert split.accepted.count() == 1
    assert split.quarantine.count() == 0


def test_trip_with_error_is_quarantined(
    spark: SparkSession,
) -> None:
    trips_df = _create_evaluated_trips(
        spark,
        [("error", ["DQ007"], [])],
    )

    split = split_trips_by_quality(trips_df)

    assert split.accepted.count() == 0
    assert split.quarantine.count() == 1


def test_quarantined_trip_preserves_errors_and_warnings(
    spark: SparkSession,
) -> None:
    trips_df = _create_evaluated_trips(
        spark,
        [("error-warning", ["DQ007"], ["DQ021"])],
    )

    split = split_trips_by_quality(trips_df)
    quarantined_trip = split.quarantine.first()

    assert split.accepted.count() == 0
    assert split.quarantine.count() == 1
    assert quarantined_trip[ERROR_RULE_IDS_COLUMN] == [
        "DQ007"
    ]
    assert quarantined_trip[WARNING_RULE_IDS_COLUMN] == [
        "DQ021"
    ]


def test_mixed_records_are_split_into_expected_branches(
    spark: SparkSession,
) -> None:
    trips_df = _create_evaluated_trips(
        spark,
        [
            ("valid", [], []),
            ("warning", [], ["DQ021"]),
            ("error", ["DQ007"], []),
            (
                "error-warning",
                ["DQ011"],
                ["DQ021"],
            ),
        ],
    )

    split = split_trips_by_quality(trips_df)
    accepted_ids = {
        row.record_id
        for row in split.accepted.select(
            "record_id"
        ).collect()
    }
    quarantine_ids = {
        row.record_id
        for row in split.quarantine.select(
            "record_id"
        ).collect()
    }

    assert accepted_ids == {"valid", "warning"}
    assert quarantine_ids == {
        "error",
        "error-warning",
    }


def test_split_does_not_lose_or_duplicate_records(
    spark: SparkSession,
) -> None:
    trips_df = _create_evaluated_trips(
        spark,
        [
            ("valid", [], []),
            ("warning", [], ["DQ021"]),
            ("error", ["DQ007"], []),
            (
                "error-warning",
                ["DQ011"],
                ["DQ021"],
            ),
        ],
    )

    split = split_trips_by_quality(trips_df)

    assert (
        split.accepted.count()
        + split.quarantine.count()
        == trips_df.count()
    )


def test_split_preserves_input_schema(
    spark: SparkSession,
) -> None:
    trips_df = _create_evaluated_trips(
        spark,
        [("valid", [], [])],
    )

    split = split_trips_by_quality(trips_df)

    assert split.accepted.schema == trips_df.schema
    assert split.quarantine.schema == trips_df.schema
    assert split.accepted.columns == trips_df.columns
    assert split.quarantine.columns == trips_df.columns


def test_missing_error_rule_ids_column_raises_error(
    spark: SparkSession,
) -> None:
    schema_without_errors = StructType([
        StructField(
            "record_id",
            StringType(),
            False,
        ),
        StructField(
            WARNING_RULE_IDS_COLUMN,
            RULE_IDS_TYPE,
            False,
        ),
    ])
    trips_df = spark.createDataFrame(
        [("missing-errors", [])],
        schema_without_errors,
    )

    with pytest.raises(
        ValueError,
        match=ERROR_RULE_IDS_COLUMN,
    ):
        split_trips_by_quality(trips_df)
