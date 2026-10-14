
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from nyc_taxi_data_platform.quality_rules import (
    ERROR_RULE_IDS_COLUMN,
    WARNING_RULE_IDS_COLUMN,
)

from nyc_taxi_data_platform.schemas import (
    TRIP_SOURCE_TO_CANONICAL,
)
from nyc_taxi_data_platform.transformations import (
    SOURCE_RECORD_COLUMN,
    SOURCE_RECORD_ID_COLUMN,
)


QUARANTINE_REQUIRED_COLUMNS = frozenset({
    SOURCE_RECORD_COLUMN,
    SOURCE_RECORD_ID_COLUMN,
    ERROR_RULE_IDS_COLUMN,
    WARNING_RULE_IDS_COLUMN,
})


def build_quarantine_trips(
    quarantine_df: DataFrame,
    *,
    processing_month: str,
    source_file: str,
    contract_version: str,
    quarantined_at: datetime,
) -> DataFrame:
    missing_columns = (
        QUARANTINE_REQUIRED_COLUMNS
        - set(quarantine_df.columns)
    )

    if missing_columns:
        raise ValueError(
            "Cannot build quarantine trips; missing "
            "required columns: "
            f"{sorted(missing_columns)}"
        )

    source_columns = [
        F.col(
            f"{SOURCE_RECORD_COLUMN}.`{source_name}`"
        ).alias(source_name)
        for source_name in TRIP_SOURCE_TO_CANONICAL
    ]
    return quarantine_df.select(
        *source_columns,
        F.col(SOURCE_RECORD_ID_COLUMN),
        F.col(ERROR_RULE_IDS_COLUMN),
        F.col(WARNING_RULE_IDS_COLUMN),
        F.size(
            F.col(ERROR_RULE_IDS_COLUMN)
        ).alias("error_count"),
        F.lit(processing_month).alias(
            "processing_month"
        ),
        F.lit(source_file).alias(
            "source_file"
        ),
        F.lit(contract_version).alias(
            "contract_version"
        ),
        F.lit(quarantined_at).alias(
            "quarantined_at"
        ),
    )
