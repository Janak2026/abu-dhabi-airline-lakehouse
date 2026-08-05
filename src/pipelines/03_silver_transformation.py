# Databricks Lakeflow Declarative Pipeline source
# Silver transformation and data quality layer for the Abu Dhabi Airline Lakehouse.
# Standardizes Bronze data, validates business keys, resolves duplicate aircraft
# reference records, and routes rejected records to the Quarantine schema.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# CONFIGURATION
# ============================================================

CATALOG = "abu_dhabi_airline"
BRONZE_SCHEMA = "bronze"
QUARANTINE_SCHEMA = "quarantine"


# ============================================================
# COMMON HELPERS
# ============================================================

def bronze_stream(table):
    return spark.readStream.table(
        f"{CATALOG}.{BRONZE_SCHEMA}.bronze_{table}"
    )


def bronze_batch(table):
    return spark.read.table(
        f"{CATALOG}.{BRONZE_SCHEMA}.bronze_{table}"
    )


def is_present(column):
    return (
        F.col(column).isNotNull()
        & (F.trim(F.col(column)) != "")
    )


def silver_timestamp(df):
    return df.withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )


def quarantine_timestamp(df):
    return df.withColumn(
        "_quarantined_at",
        F.current_timestamp()
    )


# ============================================================
# STANDARDIZATION
# ============================================================

def standardize_airports(df):
    return (
        df
        .withColumn(
            "airport_code",
            F.upper(F.trim("airport_code"))
        )
        .withColumn(
            "city",
            F.trim("city")
        )
        .withColumn(
            "country",
            F.trim("country")
        )
    )


def standardize_aircraft(df):
    return (
        df
        .withColumn(
            "aircraft_id",
            F.upper(F.trim("aircraft_id"))
        )
        .withColumn(
            "aircraft_type",
            F.trim("aircraft_type")
        )
    )


def standardize_flights(df):
    return (
        df
        .withColumn(
            "flight_id",
            F.upper(F.trim("flight_id"))
        )
        .withColumn(
            "origin",
            F.upper(F.trim("origin"))
        )
        .withColumn(
            "destination",
            F.upper(F.trim("destination"))
        )
        .withColumn(
            "aircraft_id",
            F.upper(F.trim("aircraft_id"))
        )
        .withColumn(
            "flight_status",
            F.upper(F.trim("flight_status"))
        )
    )


def standardize_bookings(df):
    return (
        df
        .withColumn(
            "booking_id",
            F.upper(F.trim("booking_id"))
        )
        .withColumn(
            "flight_id",
            F.upper(F.trim("flight_id"))
        )
    )


# ============================================================
# AIRPORTS
# ============================================================

@dp.table(
    name="silver_airports",
    comment="Validated and standardized airport reference data."
)
def silver_airports():

    df = standardize_airports(
        bronze_stream("airports")
    )

    return silver_timestamp(
        df.filter(is_present("airport_code"))
    )


@dp.table(
    name=f"{CATALOG}.{QUARANTINE_SCHEMA}.quarantine_airports",
    comment="Airport records rejected by Silver data quality validation."
)
def quarantine_airports():

    df = standardize_airports(
        bronze_stream("airports")
    )

    return quarantine_timestamp(
        df
        .filter(~is_present("airport_code"))
        .withColumn(
            "_dq_reason",
            F.lit("Missing airport_code")
        )
    )


# ============================================================
# AIRCRAFT
# Reference data: one canonical row per aircraft_id
# ============================================================

AIRCRAFT_WINDOW = (
    Window
    .partitionBy("aircraft_id")
    .orderBy(
        F.col("aircraft_type").asc_nulls_last()
    )
)


def ranked_aircraft():

    return (
        standardize_aircraft(
            bronze_batch("aircraft")
        )
        .withColumn(
            "_dq_rank",
            F.row_number().over(AIRCRAFT_WINDOW)
        )
    )


@dp.materialized_view(
    name="silver_aircraft",
    comment="Validated and deduplicated aircraft reference data."
)
def silver_aircraft():

    df = ranked_aircraft()

    return silver_timestamp(
        df
        .filter(
            is_present("aircraft_id")
            & (F.col("_dq_rank") == 1)
        )
        .drop("_dq_rank")
    )


@dp.materialized_view(
    name=f"{CATALOG}.{QUARANTINE_SCHEMA}.quarantine_aircraft",
    comment="Aircraft records rejected due to missing or duplicate aircraft identifiers."
)
def quarantine_aircraft():

    df = ranked_aircraft()

    rejected = (
        ~is_present("aircraft_id")
        | (F.col("_dq_rank") > 1)
    )

    return quarantine_timestamp(
        df
        .filter(rejected)
        .withColumn(
            "_dq_reason",
            F.when(
                ~is_present("aircraft_id"),
                F.lit("Missing aircraft_id")
            )
            .otherwise(
                F.lit("Duplicate aircraft_id")
            )
        )
        .drop("_dq_rank")
    )


# ============================================================
# FLIGHTS
# ============================================================

FLIGHT_VALID = (
    is_present("flight_id")
    & is_present("origin")
    & is_present("destination")
    & is_present("aircraft_id")
)


@dp.table(
    name="silver_flights",
    comment="Validated and standardized airline flight operational data."
)
def silver_flights():

    df = standardize_flights(
        bronze_stream("flights")
    )

    return silver_timestamp(
        df.filter(FLIGHT_VALID)
    )


@dp.table(
    name=f"{CATALOG}.{QUARANTINE_SCHEMA}.quarantine_flights",
    comment="Flight records rejected by Silver data quality validation."
)
def quarantine_flights():

    df = standardize_flights(
        bronze_stream("flights")
    )

    return quarantine_timestamp(
        df
        .filter(~FLIGHT_VALID)
        .withColumn(
            "_dq_reason",
            F.when(
                ~is_present("flight_id"),
                F.lit("Missing flight_id")
            )
            .when(
                ~is_present("origin"),
                F.lit("Missing origin")
            )
            .when(
                ~is_present("destination"),
                F.lit("Missing destination")
            )
            .when(
                ~is_present("aircraft_id"),
                F.lit("Missing aircraft_id")
            )
            .otherwise(
                F.lit("Unknown DQ violation")
            )
        )
    )


# ============================================================
# BOOKINGS
# ============================================================

BOOKING_VALID = (
    is_present("booking_id")
    & is_present("flight_id")
)


@dp.table(
    name="silver_bookings",
    comment="Validated and standardized airline passenger booking data."
)
def silver_bookings():

    df = standardize_bookings(
        bronze_stream("bookings")
    )

    return silver_timestamp(
        df.filter(BOOKING_VALID)
    )


@dp.table(
    name=f"{CATALOG}.{QUARANTINE_SCHEMA}.quarantine_bookings",
    comment="Booking records rejected by Silver data quality validation."
)
def quarantine_bookings():

    df = standardize_bookings(
        bronze_stream("bookings")
    )

    return quarantine_timestamp(
        df
        .filter(~BOOKING_VALID)
        .withColumn(
            "_dq_reason",
            F.when(
                ~is_present("booking_id"),
                F.lit("Missing booking_id")
            )
            .when(
                ~is_present("flight_id"),
                F.lit("Missing flight_id")
            )
            .otherwise(
                F.lit("Unknown DQ violation")
            )
        )
    )