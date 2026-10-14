import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)

from nyc_taxi_data_platform.quality_rules import (
    DUPLICATE_ERROR_RULE_IDS_COLUMN,
    ERROR_RULE_IDS_COLUMN,
    INTERNAL_ERROR_RULE_IDS_COLUMNS,
    SCALAR_ERROR_RULE_IDS_COLUMN,
    TEMPORAL_ERROR_RULE_IDS_COLUMN,
    WARNING_RULE_IDS_COLUMN,
    ZONE_REFERENCE_ERROR_RULE_IDS_COLUMN,
    finalize_quality_rule_ids,
)


RULE_IDS_TYPE = ArrayType(
    StringType(),
    containsNull=False,
)


def _quality_rule_df(
    spark: SparkSession,
    *,
    temporal: list[str] | None = None,
    scalar: list[str] | None = None,
    zone: list[str] | None = None,
    duplicate: list[str] | None = None,
    warnings: list[str] | None = None,
    record_ids: list[str] | None = None,
    excluded_columns: set[str] | None = None,
) -> DataFrame:
    excluded_columns = excluded_columns or set()
    record_ids = record_ids or ["record-1"]

    rule_values = {
        TEMPORAL_ERROR_RULE_IDS_COLUMN:
            temporal or [],
        SCALAR_ERROR_RULE_IDS_COLUMN:
            scalar or [],
        ZONE_REFERENCE_ERROR_RULE_IDS_COLUMN:
            zone or [],
        DUPLICATE_ERROR_RULE_IDS_COLUMN:
            duplicate or [],
        WARNING_RULE_IDS_COLUMN:
            warnings or [],
    }

    schema = StructType([
        StructField(
            "record_id",
            StringType(),
            False,
        ),
        *[
            StructField(
                column_name,
                RULE_IDS_TYPE,
                False,
            )
            for column_name in rule_values
            if column_name not in excluded_columns
        ],
    ])

    return spark.createDataFrame(
        [
            {
                "record_id": record_id,
                **rule_values,
            }
            for record_id in record_ids
        ],
        schema,
    )


def test_no_violations_produces_empty_arrays(
    spark: SparkSession,
) -> None:
    trips_df = _quality_rule_df(spark)

    result = finalize_quality_rule_ids(
        trips_df
    ).first()

    assert result[ERROR_RULE_IDS_COLUMN] == []
    assert result[WARNING_RULE_IDS_COLUMN] == []


def test_all_error_groups_are_merged_and_sorted(
    spark: SparkSession,
) -> None:
    trips_df = _quality_rule_df(
        spark,
        temporal=["DQ003"],
        scalar=["DQ025", "DQ007"],
        zone=["DQ011"],
        duplicate=["DQ013"],
    )

    result = finalize_quality_rule_ids(
        trips_df
    ).first()

    assert result[ERROR_RULE_IDS_COLUMN] == [
        "DQ003",
        "DQ007",
        "DQ011",
        "DQ013",
        "DQ025",
    ]


def test_duplicate_error_ids_are_removed(
    spark: SparkSession,
) -> None:
    trips_df = _quality_rule_df(
        spark,
        temporal=["DQ003"],
        scalar=["DQ003", "DQ007"],
    )

    result = finalize_quality_rule_ids(
        trips_df
    ).first()

    assert result[ERROR_RULE_IDS_COLUMN] == [
        "DQ003",
        "DQ007",
    ]


def test_warning_ids_are_distinct_and_sorted(
    spark: SparkSession,
) -> None:
    trips_df = _quality_rule_df(
        spark,
        warnings=["DQ021", "DQ016", "DQ021"],
    )

    result = finalize_quality_rule_ids(
        trips_df
    ).first()

    assert result[WARNING_RULE_IDS_COLUMN] == [
        "DQ016",
        "DQ021",
    ]


def test_internal_rule_columns_are_removed(
    spark: SparkSession,
) -> None:
    trips_df = _quality_rule_df(spark)

    result_df = finalize_quality_rule_ids(
        trips_df
    )

    assert "record_id" in result_df.columns
    assert ERROR_RULE_IDS_COLUMN in result_df.columns
    assert WARNING_RULE_IDS_COLUMN in result_df.columns

    for column_name in (
        INTERNAL_ERROR_RULE_IDS_COLUMNS
    ):
        assert column_name not in result_df.columns


def test_finalization_preserves_record_count(
    spark: SparkSession,
) -> None:
    trips_df = _quality_rule_df(
        spark,
        record_ids=[
            "record-1",
            "record-2",
            "record-3",
        ],
    )

    result_df = finalize_quality_rule_ids(
        trips_df
    )

    assert result_df.count() == trips_df.count()


@pytest.mark.parametrize(
    "missing_column",
    [
        *INTERNAL_ERROR_RULE_IDS_COLUMNS,
        WARNING_RULE_IDS_COLUMN,
    ],
)
def test_missing_rule_column_raises_error(
    spark: SparkSession,
    missing_column: str,
) -> None:
    trips_df = _quality_rule_df(
        spark,
        excluded_columns={missing_column},
    )

    with pytest.raises(
        ValueError,
        match=missing_column,
    ):
        finalize_quality_rule_ids(trips_df)
