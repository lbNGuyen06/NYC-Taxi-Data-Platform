import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

from nyc_taxi_data_platform.quality_rules import (
    DUPLICATE_ERROR_RULE_IDS_COLUMN,
    apply_duplicate_quality_rule,
)


DUPLICATE_TEST_SCHEMA = StructType([
    StructField(
        "source_record_id",
        StringType(),
        False,
    ),
    StructField(
        "payload",
        StringType(),
        False,
    ),
])


def _create_trips_df(
    spark: SparkSession,
    source_record_ids: list[str],
) -> DataFrame:
    return spark.createDataFrame(
        [
            (source_record_id, "payload")
            for source_record_id
            in source_record_ids
        ],
        DUPLICATE_TEST_SCHEMA,
    )


def _rule_counts(
    result_df: DataFrame,
) -> dict[tuple[str, ...], int]:
    return {
        tuple(
            row[DUPLICATE_ERROR_RULE_IDS_COLUMN]
        ): row["count"]
        for row in result_df.groupBy(
            DUPLICATE_ERROR_RULE_IDS_COLUMN
        ).count().collect()
    }


def test_distinct_source_record_ids_are_not_duplicates(
    spark: SparkSession,
) -> None:
    trips_df = _create_trips_df(
        spark,
        ["id-1", "id-2", "id-3"],
    )

    result_df = apply_duplicate_quality_rule(
        trips_df
    )

    rule_ids_by_record_id = {
        row["source_record_id"]:
            row[DUPLICATE_ERROR_RULE_IDS_COLUMN]
        for row in result_df.collect()
    }

    assert rule_ids_by_record_id == {
        "id-1": [],
        "id-2": [],
        "id-3": [],
    }


def test_second_record_with_same_id_is_duplicate(
    spark: SparkSession,
) -> None:
    trips_df = _create_trips_df(
        spark,
        ["id-1", "id-1"],
    )

    result_df = apply_duplicate_quality_rule(
        trips_df
    )

    assert _rule_counts(result_df) == {
        (): 1,
        ("DQ013",): 1,
    }


def test_three_records_with_same_id_mark_two_duplicates(
    spark: SparkSession,
) -> None:
    trips_df = _create_trips_df(
        spark,
        ["id-1", "id-1", "id-1"],
    )

    result_df = apply_duplicate_quality_rule(
        trips_df
    )

    assert _rule_counts(result_df) == {
        (): 1,
        ("DQ013",): 2,
    }


def test_multiple_duplicate_groups(
    spark: SparkSession,
) -> None:
    trips_df = _create_trips_df(
        spark,
        [
            "id-1",
            "id-1",
            "id-2",
            "id-2",
            "id-2",
            "id-3",
        ],
    )

    result_df = apply_duplicate_quality_rule(
        trips_df
    )

    assert _rule_counts(result_df) == {
        (): 3,
        ("DQ013",): 3,
    }


def test_duplicate_rule_preserves_rows_and_columns(
    spark: SparkSession,
) -> None:
    trips_df = _create_trips_df(
        spark,
        ["id-1", "id-1", "id-2"],
    )

    result_df = apply_duplicate_quality_rule(
        trips_df
    )

    assert result_df.count() == trips_df.count()
    assert result_df.columns == [
        *trips_df.columns,
        DUPLICATE_ERROR_RULE_IDS_COLUMN,
    ]


def test_duplicate_rule_requires_source_record_id(
    spark: SparkSession,
) -> None:
    trips_df = spark.createDataFrame(
        [("payload",)],
        StructType([
            StructField(
                "payload",
                StringType(),
                False,
            ),
        ]),
    )

    with pytest.raises(
        ValueError,
        match="source_record_id",
    ):
        apply_duplicate_quality_rule(trips_df)
