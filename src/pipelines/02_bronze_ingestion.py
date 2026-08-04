# Databricks notebook source
# Lakeflow Declarative Pipeline - Bronze ingestion.
# Incrementally ingests airline CSV datasets from a Unity Catalog Volume
# using Auto Loader and persists governed Bronze Delta tables with
# ingestion metadata for lineage and operational traceability.

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp


SOURCE_BASE = "/Volumes/abu_dhabi_airline/default/source_data"


def read_source(dataset):
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{SOURCE_BASE}/{dataset}")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )


@dp.table(
    name="bronze_airports",
    comment="Raw airport reference data incrementally ingested from the source volume."
)
def bronze_airports():
    return read_source("airports")


@dp.table(
    name="bronze_aircraft",
    comment="Raw aircraft reference data incrementally ingested from the source volume."
)
def bronze_aircraft():
    return read_source("aircraft")


@dp.table(
    name="bronze_flights",
    comment="Raw flight operational data incrementally ingested from the source volume."
)
def bronze_flights():
    return read_source("flights")


@dp.table(
    name="bronze_bookings",
    comment="Raw booking data incrementally ingested from the source volume."
)
def bronze_bookings():
    return read_source("bookings")