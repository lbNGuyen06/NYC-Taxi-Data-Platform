from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

TRIP_REQUIRED_COLUMNS = frozenset({
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
})


TRIP_OPTIONAL_COLUMNS = frozenset({
    "passenger_count",
    "RatecodeID",
    "store_and_fwd_flag",
    "congestion_surcharge",
    "Airport_fee",
    "cbd_congestion_fee",
})

TRIP_EXPECTED_TYPES = {
    "VendorID": IntegerType(),
    "tpep_pickup_datetime": TimestampNTZType(),
    "tpep_dropoff_datetime": TimestampNTZType(),
    "passenger_count": LongType(),
    "trip_distance": DoubleType(),
    "PULocationID": IntegerType(),
    "DOLocationID": IntegerType(),
    "payment_type": LongType(),
    "fare_amount": DoubleType(),
    "extra": DoubleType(),
    "mta_tax": DoubleType(),
    "tip_amount": DoubleType(),
    "tolls_amount": DoubleType(),
    "improvement_surcharge": DoubleType(),
    "total_amount": DoubleType(),
    "RatecodeID": LongType(),
    "store_and_fwd_flag": StringType(),
    "congestion_surcharge": DoubleType(),
    "Airport_fee": DoubleType(),
    "cbd_congestion_fee": DoubleType(),
}
ZONE_SOURCE_SCHEMA = StructType([
    StructField("LocationID", IntegerType(), True),
    StructField("Borough", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("service_zone", StringType(), True),
])
