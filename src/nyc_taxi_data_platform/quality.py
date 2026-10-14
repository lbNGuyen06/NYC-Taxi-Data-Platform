from typing import NamedTuple

from pyspark.sql import DataFrame

from pyspark.sql import functions as F

from nyc_taxi_data_platform.quality_rules import (
    ERROR_RULE_IDS_COLUMN,
)


class QualitySplit(NamedTuple):
    accepted: DataFrame
    quarantine: DataFrame


def split_trips_by_quality(
    trips_df: DataFrame,
) -> QualitySplit:
    if ERROR_RULE_IDS_COLUMN not in trips_df.columns:
        raise ValueError(
            "Cannot split trips by quality: "
            f"missing column {ERROR_RULE_IDS_COLUMN!r}"
        )

    has_errors = (
        F.size(F.col(ERROR_RULE_IDS_COLUMN)) > 0
    )

    accepted_df = trips_df.filter(
        ~has_errors
    )
    quarantine_df = trips_df.filter(
        has_errors
    )

    return QualitySplit(
        accepted=accepted_df,
        quarantine=quarantine_df,
    )
