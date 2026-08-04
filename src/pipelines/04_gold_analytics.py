# Databricks Lakeflow Declarative Pipeline source
# Gold serving layer for the Abu Dhabi Airline Lakehouse.
# Implements dimensional modeling and business-ready analytical datasets
# from curated Silver data for governed downstream AI/BI and Genie consumption.

from pyspark import pipelines as dp
from pyspark.sql import functions as F


CATALOG = "abu_dhabi_airline"
SILVER_SCHEMA = "silver"


# ============================================================
# DIMENSIONAL MODEL
# ============================================================


# ============================================================
# DIM AIRPORT
# Grain: one row per airport
# ============================================================

@dp.materialized_view(
    name="dim_airport",
    comment="Conformed airport dimension for airline analytics."
)
def dim_airport():

    airports = spark.read.table(
        f"{CATALOG}.{SILVER_SCHEMA}.silver_airports"
    )

    return (
        airports
        .dropDuplicates(["airport_code"])
    )


# ============================================================
# DIM AIRCRAFT
# Grain: one row per aircraft
# ============================================================

@dp.materialized_view(
    name="dim_aircraft",
    comment="Conformed aircraft dimension for airline analytics."
)
def dim_aircraft():

    aircraft = spark.read.table(
        f"{CATALOG}.{SILVER_SCHEMA}.silver_aircraft"
    )

    return (
        aircraft
        .dropDuplicates(["aircraft_id"])
    )


# ============================================================
# FACT FLIGHTS
# Grain: one row per flight
# ============================================================

@dp.materialized_view(
    name="fact_flights",
    comment="Core flight fact table containing operational measures and dimension references."
)
def fact_flights():

    flights = spark.read.table(
        f"{CATALOG}.{SILVER_SCHEMA}.silver_flights"
    )

    return (
        flights
        .select(
            "flight_id",
            "origin",
            "destination",
            "aircraft_id",
            "scheduled_departure",
            "delay_minutes",
            "flight_status"
        )
        .withColumn(
            "is_delayed",
            F.when(F.col("delay_minutes") > 0, 1).otherwise(0)
        )
        .withColumn(
            "route",
            F.concat_ws(
                "-",
                F.col("origin"),
                F.col("destination")
            )
        )
    )


# ============================================================
# BUSINESS ANALYTICAL DATASETS
# ============================================================


# ============================================================
# GOLD FLIGHT PERFORMANCE
# Grain: one row per flight
# ============================================================

@dp.materialized_view(
    name="gold_flight_performance",
    comment="Business-ready flight-level operational performance dataset."
)
def gold_flight_performance():

    flights = spark.read.table(
        f"{CATALOG}.{SILVER_SCHEMA}.silver_flights"
    )

    return (
        flights
        .select(
            "flight_id",
            "origin",
            "destination",
            "aircraft_id",
            "scheduled_departure",
            "delay_minutes",
            "flight_status"
        )
        .withColumn(
            "is_delayed",
            F.when(F.col("delay_minutes") > 0, 1).otherwise(0)
        )
        .withColumn(
            "route",
            F.concat_ws(
                "-",
                F.col("origin"),
                F.col("destination")
            )
        )
    )


# ============================================================
# GOLD ROUTE PERFORMANCE
# Grain: one row per origin-destination route
# ============================================================

@dp.materialized_view(
    name="gold_route_performance",
    comment="Route-level operational KPIs including flights, delays and cancellations."
)
def gold_route_performance():

    flights = spark.read.table(
        f"{CATALOG}.{SILVER_SCHEMA}.silver_flights"
    )

    return (
        flights
        .groupBy(
            "origin",
            "destination"
        )
        .agg(
            F.count("*").alias("total_flights"),

            F.round(
                F.avg("delay_minutes"),
                2
            ).alias("avg_delay_minutes"),

            F.sum(
                F.when(
                    F.col("delay_minutes") > 0,
                    1
                ).otherwise(0)
            ).alias("delayed_flights"),

            F.sum(
                F.when(
                    F.col("flight_status") == "CANCELLED",
                    1
                ).otherwise(0)
            ).alias("cancelled_flights")
        )
        .withColumn(
            "delay_rate_pct",
            F.round(
                F.col("delayed_flights")
                / F.col("total_flights")
                * 100,
                2
            )
        )
    )


# ============================================================
# GOLD AIRCRAFT PERFORMANCE
# Grain: one row per aircraft
# ============================================================

@dp.materialized_view(
    name="gold_aircraft_performance",
    comment="Aircraft-level utilization and operational performance KPIs."
)
def gold_aircraft_performance():

    flights = spark.read.table(
        f"{CATALOG}.{SILVER_SCHEMA}.silver_flights"
    )

    aircraft = spark.read.table(
        f"{CATALOG}.{SILVER_SCHEMA}.silver_aircraft"
    )

    flight_metrics = (
        flights
        .groupBy("aircraft_id")
        .agg(
            F.count("*").alias("total_flights"),

            F.round(
                F.avg("delay_minutes"),
                2
            ).alias("avg_delay_minutes"),

            F.sum(
                F.when(
                    F.col("delay_minutes") > 0,
                    1
                ).otherwise(0)
            ).alias("delayed_flights")
        )
    )

    return (
        aircraft
        .join(
            flight_metrics,
            on="aircraft_id",
            how="left"
        )
        .fillna(
            {
                "total_flights": 0,
                "delayed_flights": 0
            }
        )
    )


# ============================================================
# GOLD BOOKING SUMMARY
# Grain: one row per flight
# ============================================================

@dp.materialized_view(
    name="gold_booking_summary",
    comment="Flight-level booking KPIs for airline demand analytics."
)
def gold_booking_summary():

    bookings = spark.read.table(
        f"{CATALOG}.{SILVER_SCHEMA}.silver_bookings"
    )

    return (
        bookings
        .groupBy("flight_id")
        .agg(
            F.count("*").alias("total_bookings")
        )
    )