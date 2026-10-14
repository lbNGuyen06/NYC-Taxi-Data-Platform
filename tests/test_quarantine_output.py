from datetime import datetime

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)

from nyc_taxi_data_platform.outputs import (
    build_quarantine_trips,
)
from nyc_taxi_data_platform.quality_rules import (
    ERROR_RULE_IDS_COLUMN,
    WARNING_RULE_IDS_COLUMN,
)
from nyc_taxi_data_platform.schemas import (
    TRIP_EXPECTED_TYPES,
    TRIP_SOURCE_TO_CANONICAL,
)
from nyc_taxi_data_platform.transformations import (
    SOURCE_RECORD_COLUMN,
    SOURCE_RECORD_ID_COLUMN,
)


RULE_IDS_TYPE = ArrayType(
    StringType(),
    containsNull=False,
)
SOURCE_RECORD_SCHEMA = StructType([
    StructField(name, data_type, True)
    for name, data_type in TRIP_EXPECTED_TYPES.items()
])
QUARANTINE_INPUT_SCHEMA = StructType([
    StructField(
        SOURCE_RECORD_COLUMN,
        SOURCE_RECORD_SCHEMA,
        False,
    ),
    StructField(
        SOURCE_RECORD_ID_COLUMN,
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
    StructField(
        "canonical_only_marker",
        StringType(),
        True,
    ),
])

PROCESSING_MONTH = "2025-01"
SOURCE_FILE = "yellow_tripdata_2025-01.parquet"
CONTRACT_VERSION = "0.2.0"
QUARANTINED_AT = datetime(2026, 10, 14, 9, 30)


def _source_record() -> dict:
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


def _quarantine_df(
    spark: SparkSession,
    *,
    records: list[tuple] | None = None,
) -> DataFrame:
    records = records or [
        (
            _source_record(),
            "record-1",
            ["DQ003", "DQ007", "DQ011"],
            ["DQ021"],
            "must-not-be-published",
        )
    ]
    return spark.createDataFrame(
        records,
        QUARANTINE_INPUT_SCHEMA,
    )


def _build_output(quarantine_df: DataFrame) -> DataFrame:
    return build_quarantine_trips(
        quarantine_df,
        processing_month=PROCESSING_MONTH,
        source_file=SOURCE_FILE,
        contract_version=CONTRACT_VERSION,
        quarantined_at=QUARANTINED_AT,
    )


def test_quarantine_output_has_expected_column_order(
    spark: SparkSession,
) -> None:
    result_df = _build_output(_quarantine_df(spark))

    assert result_df.columns == [
        *TRIP_SOURCE_TO_CANONICAL.keys(),
        SOURCE_RECORD_ID_COLUMN,
        ERROR_RULE_IDS_COLUMN,
        WARNING_RULE_IDS_COLUMN,
        "error_count",
        "processing_month",
        "source_file",
        "contract_version",
        "quarantined_at",
    ]


def test_quarantine_output_preserves_raw_source_values(
    spark: SparkSession,
) -> None:
    result = _build_output(
        _quarantine_df(spark)
    ).first()

    assert result.store_and_fwd_flag == " y "
    assert result.trip_distance == 1.25


def test_quarantine_output_calculates_error_count(
    spark: SparkSession,
) -> None:
    result = _build_output(
        _quarantine_df(spark)
    ).first()

    assert result.error_count == 3


def test_quarantine_output_preserves_rule_ids(
    spark: SparkSession,
) -> None:
    result = _build_output(
        _quarantine_df(spark)
    ).first()

    assert result.error_rule_ids == [
        "DQ003",
        "DQ007",
        "DQ011",
    ]
    assert result.warning_rule_ids == ["DQ021"]


def test_quarantine_output_adds_run_metadata(
    spark: SparkSession,
) -> None:
    result = _build_output(
        _quarantine_df(spark)
    ).first()

    assert result.processing_month == PROCESSING_MONTH
    assert result.source_file == SOURCE_FILE
    assert result.contract_version == CONTRACT_VERSION
    assert result.quarantined_at == QUARANTINED_AT


def test_quarantine_output_drops_canonical_and_internal_columns(
    spark: SparkSession,
) -> None:
    result_df = _build_output(_quarantine_df(spark))

    assert "canonical_only_marker" not in result_df.columns
    assert SOURCE_RECORD_COLUMN not in result_df.columns


def test_quarantine_output_preserves_record_count(
    spark: SparkSession,
) -> None:
    second_source_record = _source_record()
    second_source_record["VendorID"] = 2
    quarantine_df = _quarantine_df(
        spark,
        records=[
            (
                _source_record(),
                "record-1",
                ["DQ007"],
                [],
                "first",
            ),
            (
                second_source_record,
                "record-2",
                ["DQ011"],
                ["DQ021"],
                "second",
            ),
        ],
    )

    result_df = _build_output(quarantine_df)

    assert result_df.count() == quarantine_df.count()


@pytest.mark.parametrize(
    "missing_column",
    [
        SOURCE_RECORD_COLUMN,
        SOURCE_RECORD_ID_COLUMN,
        ERROR_RULE_IDS_COLUMN,
        WARNING_RULE_IDS_COLUMN,
    ],
)
def test_missing_required_input_column_raises_error(
    spark: SparkSession,
    missing_column: str,
) -> None:
    quarantine_df = _quarantine_df(spark).drop(
        missing_column
    )

    with pytest.raises(
        ValueError,
        match=missing_column,
    ):
        _build_output(quarantine_df)
