# Databricks notebook source
# Generates lightweight synthetic airline source datasets for the
# Abu Dhabi Airline Lakehouse project. Data is intentionally small and
# represents upstream operational systems before Lakeflow ingestion.

from pyspark.sql import functions as F
from datetime import datetime, timedelta
import random

BASE_PATH = "/Volumes/abu_dhabi_airline/default/source_data"

random.seed(42)

# COMMAND ----------

# AIRPORTS

airports_data = [
    ("AUH", "Abu Dhabi", "UAE"),
    ("DXB", "Dubai", "UAE"),
    ("DOH", "Doha", "Qatar"),
    ("MCT", "Muscat", "Oman"),
    ("DEL", "Delhi", "India"),
    ("BOM", "Mumbai", "India"),
    ("HYD", "Hyderabad", "India"),
    ("BLR", "Bengaluru", "India"),
    ("MAA", "Chennai", "India"),
    ("COK", "Kochi", "India"),
    ("LHR", "London", "UK"),
    ("CDG", "Paris", "France"),
    ("FRA", "Frankfurt", "Germany"),
    ("JFK", "New York", "USA"),
    ("SIN", "Singapore", "Singapore")
]

airports = spark.createDataFrame(
    airports_data,
    ["airport_code", "city", "country"]
)

# COMMAND ----------

# AIRCRAFT

aircraft_types = [
    "Airbus A320",
    "Airbus A321",
    "Airbus A350",
    "Boeing 777",
    "Boeing 787"
]

aircraft_data = [
    (
        f"AC{i:03}",
        random.choice(aircraft_types),
        random.randint(180, 350)
    )
    for i in range(1, 26)
]

aircraft = spark.createDataFrame(
    aircraft_data,
    ["aircraft_id", "aircraft_type", "capacity"]
)

# COMMAND ----------

# FLIGHTS

airport_codes = [x[0] for x in airports_data]
destinations = [x for x in airport_codes if x != "AUH"]
aircraft_ids = [f"AC{i:03}" for i in range(1, 26)]

start_date = datetime(2026, 1, 1)

flight_data = []

for i in range(1, 10001):

    departure = start_date + timedelta(
        days=random.randint(0, 180),
        hours=random.randint(0, 23),
        minutes=random.choice([0, 15, 30, 45])
    )

    delay = random.choices(
        [0, 15, 30, 60, 120],
        weights=[65, 15, 10, 7, 3]
    )[0]

    status = random.choices(
        ["ON_TIME", "DELAYED", "CANCELLED"],
        weights=[75, 22, 3]
    )[0]

    flight_data.append(
        (
            f"FL{i:05}",
            "AUH",
            random.choice(destinations),
            random.choice(aircraft_ids),
            departure,
            delay,
            status
        )
    )

flights = spark.createDataFrame(
    flight_data,
    [
        "flight_id",
        "origin",
        "destination",
        "aircraft_id",
        "scheduled_departure",
        "delay_minutes",
        "flight_status"
    ]
)

# COMMAND ----------

# BOOKINGS

booking_data = []

for i in range(1, 15001):

    booking_data.append(
        (
            f"BK{i:06}",
            f"FL{random.randint(1,10000):05}",
            random.choice(["ECONOMY", "BUSINESS", "FIRST"]),
            round(random.uniform(250, 5000), 2),
            random.choice(["CONFIRMED", "CANCELLED"])
        )
    )

bookings = spark.createDataFrame(
    booking_data,
    [
        "booking_id",
        "flight_id",
        "travel_class",
        "fare_aed",
        "booking_status"
    ]
)

# COMMAND ----------

# WRITE SOURCE FILES

datasets = {
    "airports": airports,
    "aircraft": aircraft,
    "flights": flights,
    "bookings": bookings
}

for name, df in datasets.items():

    (
        df.coalesce(1)
          .write
          .mode("overwrite")
          .option("header", True)
          .csv(f"{BASE_PATH}/{name}")
    )

    print(f"{name}: {df.count()} records written")

# COMMAND ----------

display(flights.limit(10))