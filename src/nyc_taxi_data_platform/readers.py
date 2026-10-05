from pyspark.sql import DataFrame, SparkSession
from nyc_taxi_data_platform.schemas import ZONE_SOURCE_SCHEMA


def read_trips(spark: SparkSession, path: str) -> DataFrame:
    trips_df = spark.read.parquet(path)
    return trips_df


def read_zones(spark: SparkSession, path: str) -> DataFrame:
    zones_df = (
        spark.read
        .schema(ZONE_SOURCE_SCHEMA)
        .option("header", "true")
        .option("enforceSchema", "false")
        .csv(path)
    )
    return zones_df
