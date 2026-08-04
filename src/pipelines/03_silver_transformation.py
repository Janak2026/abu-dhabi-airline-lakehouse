# Databricks pipeline source
# Silver transformation layer for the Abu Dhabi Airline Lakehouse.
# Cleans, standardizes, validates, and enriches Bronze operational datasets.

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    current_timestamp
)


CATALOG = "abu_dhabi_airline"
BRONZE_SCHEMA = "bronze"


# ============================================================
# SILVER AIRPORTS
# ============================================================

@dp.table(
    name="silver_airports",
    comment="Validated and standardized airport reference data."
)
def silver_airports():

    return (
        spark.readStream
        .table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_airports")

        # Remove invalid business keys
        .filter(col("airport_code").isNotNull())

        # Standardize values
        .withColumn("airport_code", upper(trim(col("airport_code"))))
        .withColumn("city", trim(col("city")))
        .withColumn("country", trim(col("country")))

        # Silver processing metadata
        .withColumn("_silver_processed_at", current_timestamp())
    )


# ============================================================
# SILVER AIRCRAFT
# ============================================================

@dp.table(
    name="silver_aircraft",
    comment="Validated and standardized aircraft reference data."
)
def silver_aircraft():

    return (
        spark.readStream
        .table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_aircraft")

        .filter(col("aircraft_id").isNotNull())

        .withColumn("aircraft_id", upper(trim(col("aircraft_id"))))
        .withColumn("aircraft_type", trim(col("aircraft_type")))

        .withColumn("_silver_processed_at", current_timestamp())
    )


# ============================================================
# SILVER FLIGHTS
# ============================================================

@dp.table(
    name="silver_flights",
    comment="Cleaned and standardized airline flight operational data."
)
def silver_flights():

    return (
        spark.readStream
        .table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_flights")

        .filter(col("flight_id").isNotNull())

        .withColumn("flight_id", upper(trim(col("flight_id"))))
        .withColumn("origin", upper(trim(col("origin"))))
        .withColumn("destination", upper(trim(col("destination"))))
        .withColumn("aircraft_id", upper(trim(col("aircraft_id"))))
        .withColumn("flight_status", upper(trim(col("flight_status"))))

        .withColumn("_silver_processed_at", current_timestamp())
    )


# ============================================================
# SILVER BOOKINGS
# ============================================================

@dp.table(
    name="silver_bookings",
    comment="Cleaned and standardized airline passenger booking data."
)
def silver_bookings():

    return (
        spark.readStream
        .table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_bookings")

        .filter(col("booking_id").isNotNull())
        .filter(col("flight_id").isNotNull())

        .withColumn("booking_id", upper(trim(col("booking_id"))))
        .withColumn("flight_id", upper(trim(col("flight_id"))))

        .withColumn("_silver_processed_at", current_timestamp())
    )