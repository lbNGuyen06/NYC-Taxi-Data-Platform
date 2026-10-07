import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder
        .master("local[1]")
        .appName("nyc-taxi-tests")
        .config(
            "spark.sql.session.timeZone",
            "America/New_York",
        )
        .config(
            "spark.sql.timestampType",
            "TIMESTAMP_NTZ",
        )
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()
