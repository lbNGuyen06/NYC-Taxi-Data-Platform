from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    DecimalType,
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

TRIP_SOURCE_TO_CANONICAL = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_at",
    "tpep_dropoff_datetime": "dropoff_at",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance_miles",
    "RatecodeID": "rate_code_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pickup_zone_id",
    "DOLocationID": "dropoff_zone_id",
    "payment_type": "payment_type_id",
    "fare_amount": "fare_amount",
    "extra": "extra_amount",
    "mta_tax": "mta_tax_amount",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge_amount",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge_amount",
    "Airport_fee": "airport_fee_amount",
    "cbd_congestion_fee": "cbd_congestion_fee_amount",
}
ZONE_SOURCE_TO_CANONICAL = {
    "LocationID": "zone_id",
    "Borough": "borough",
    "Zone": "zone_name",
    "service_zone": "service_zone",
}

TRIP_CANONICAL_TYPES = {
    "vendor_id": IntegerType(),
    "pickup_at": TimestampNTZType(),
    "dropoff_at": TimestampNTZType(),
    "passenger_count": IntegerType(),
    "trip_distance_miles": DecimalType(12, 2),
    "rate_code_id": IntegerType(),
    "store_and_fwd_flag": StringType(),
    "pickup_zone_id": IntegerType(),
    "dropoff_zone_id": IntegerType(),
    "payment_type_id": IntegerType(),
    "fare_amount": DecimalType(12, 2),
    "extra_amount": DecimalType(12, 2),
    "mta_tax_amount": DecimalType(12, 2),
    "tip_amount": DecimalType(12, 2),
    "tolls_amount": DecimalType(12, 2),
    "improvement_surcharge_amount": DecimalType(12, 2),
    "total_amount": DecimalType(12, 2),
    "congestion_surcharge_amount": DecimalType(12, 2),
    "airport_fee_amount": DecimalType(12, 2),
    "cbd_congestion_fee_amount": DecimalType(12, 2),
}

ZONE_CANONICAL_TYPES = {
    "zone_id": IntegerType(),
    "borough": StringType(),
    "zone_name": StringType(),
    "service_zone": StringType(),
}
