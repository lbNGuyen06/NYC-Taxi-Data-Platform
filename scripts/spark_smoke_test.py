import argparse
from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Kiểm tra Spark có thể đọc dữ liệu NYC Taxi"
    )
    parser.add_argument(
        "--trips-path",
        required=True,
        help="Đường dẫn đến file Yellow Taxi Parquet"
    )
    parser.add_argument(
        "--zones-path",
        required=True,
        help="Đường dẫn đến file Taxi Zones CSV"
    )
    args = parser.parse_args()
    return args


def create_spark_session() -> SparkSession:
    spark = SparkSession.builder.appName(
        "nyc-taxi-smoke-test").config("spark.sql.session.timeZone", "America/New_York").getOrCreate()
    return spark


def main() -> None:
    args = parse_args()
    spark = create_spark_session()
    try:
        trips = spark.read.parquet(args.trips_path)
        zones = spark.read.option("header", "true").csv(args.zones_path)
        print("\n=== schema của trips ===")
        trips.printSchema()
        print("\n=== schema của zones ===")
        zones.printSchema()
        trips_count = trips.count()
        zones_count = zones.count()
        if trips_count == 0:
            raise RuntimeError("Trip dataset is empty")

        if zones_count == 0:
            raise RuntimeError("Zone dataset is empty")
        print(f"\nTrips count: {trips_count}")
        print(f"\nZones count:{zones_count}")
        print("\n=== SAMPLE TRIPS ===")
        trips.show(5, truncate=False)

        print("\n=== SAMPLE ZONES ===")
        zones.show(5, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
