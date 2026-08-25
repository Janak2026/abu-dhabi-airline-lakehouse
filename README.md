# Abu Dhabi Airline Lakehouse

An end-to-end airline data engineering, analytics, and conversational BI platform built on Databricks.

The project demonstrates a complete data lifecycle from raw airline operational data to trusted business insights using **Lakehouse architecture, Databricks Declarative Pipelines, Delta Lake, PySpark, data quality and quarantine patterns, Gold-layer analytics, AI/BI Dashboards, and Databricks Genie**.

---

## 1. Project Objective

The objective of this project is to build a production-style airline analytics platform capable of transforming raw operational data into trusted, business-ready information.

The platform supports questions such as:

- How many flights were delayed?
- Which aircraft has the highest average delay?
- Which routes experience the highest delays?
- Which flights have the highest booking demand?
- Which routes have the highest passenger demand?
- Which high-demand routes are also experiencing poor operational performance?

The solution follows a layered Lakehouse architecture:

```text
Source Data
    │
    ▼
Bronze Layer
    │
    ▼
Silver Layer
    │
    ├── Data Quality Validation
    ├── Deduplication
    └── Quarantine
    │
    ▼
Gold Layer
    │
    ├── Flight Performance
    ├── Route Performance
    ├── Aircraft Performance
    └── Booking Analytics
    │
    ├────────────────────┐
    ▼                    ▼
AI/BI Dashboard      Genie Agent
    │                    │
    └──────────┬─────────┘
               ▼
        Business Insights
```

---

## 2. Technology Stack

| Area | Technology |
|---|---|
| Platform | Databricks |
| Storage | Delta Lake |
| Processing | Apache Spark / PySpark |
| Governance | Unity Catalog |
| Pipeline Framework | Databricks Declarative Pipelines |
| Data Architecture | Medallion Architecture |
| Data Quality | Expectations + Quarantine Pattern |
| Orchestration | Databricks Jobs & Pipelines |
| Analytics | Databricks AI/BI Dashboards |
| Conversational Analytics | Databricks Genie |
| Version Control | Git / Databricks Git Folder |

---

## 3. Repository Structure

The repository is organized around the implemented data platform components and supporting documentation.

```text
abu-dhabi-airline-lakehouse/
│
├── src/
│   ├── ingestion/
│   └── pipelines/
│
├── docs/
│   └── images/
│       ├── 05_pipeline-graph.jpg
│       ├── 06_airline-operations-dashboard.jpg
│       └── 07_genie-cross-domain-analysis.jpg
│
├── .gitignore
├── LICENSE
└── README.md
```

The repository structure intentionally reflects the implemented solution rather than maintaining unused placeholder directories.

---

## 4. Data Domains

The platform processes four core airline datasets.

### Flights

Flight-level operational information including:

- flight identifier
- origin
- destination
- aircraft
- flight status
- delay information

### Aircraft

Aircraft master information including:

- aircraft identifier
- aircraft type

### Airports

Airport reference information used to enrich operational and route data.

### Bookings

Passenger booking information associated with individual flights.

Together, these datasets support operational, route, aircraft, and passenger-demand analytics.

---

## 5. Lakehouse Architecture

The project follows the **Medallion Architecture**, separating raw ingestion, validated data, and business-ready analytics into Bronze, Silver, and Gold layers.

### Bronze Layer

The Bronze layer preserves source data with minimal transformation.

Its primary responsibilities are:

- preserving source fidelity
- enabling replay and debugging
- maintaining lineage
- separating ingestion from business transformations
- retaining source-level data quality issues for downstream handling

Bronze tables include:

```text
bronze_flights
bronze_aircraft
bronze_airports
bronze_bookings
```

Duplicates and imperfect source records are intentionally permitted at this layer.

---

### Silver Layer

The Silver layer produces cleaned, validated, and standardized operational datasets.

Key transformations include:

- schema normalization
- datatype handling
- null validation
- duplicate detection
- business-rule validation
- deterministic deduplication
- data-quality routing

Silver tables include:

```text
silver_flights
silver_aircraft
silver_airports
silver_bookings
```

Only validated records progress into downstream business analytics.

---

## 6. Data Quality and Quarantine

Invalid records are not silently discarded.

Records that violate data-quality rules are redirected to dedicated quarantine tables:

```text
quarantine_flights
quarantine_aircraft
quarantine_airports
quarantine_bookings
```

Each quarantined record retains information explaining why the record was rejected.

Example:

```text
aircraft_id | aircraft_type | _dq_reason
------------|---------------|----------------------
AC001       | Airbus A321   | Duplicate aircraft_id
```

This pattern preserves problematic data for:

- investigation
- auditing
- debugging
- root-cause analysis
- source-system remediation

while ensuring downstream analytics operate on trusted records.

---

## 7. Aircraft Duplicate Handling

During data validation, duplicate `aircraft_id` values were detected in the aircraft source.

Some identifiers contained identical aircraft information, while others contained conflicting aircraft types.

Example:

```text
AC001 | Airbus A320
AC001 | Airbus A321
```

The Bronze layer intentionally retained both records because Bronze represents the original source state.

The Silver transformation then applied deterministic deduplication so that:

```text
1 aircraft_id → 1 Silver aircraft record
```

Rejected duplicate records were redirected to:

```text
quarantine_aircraft
```

Validation confirmed:

```text
Silver aircraft records: 25
Duplicate aircraft IDs: 0
```

This demonstrates the separation of responsibilities between raw ingestion and trusted analytical data.

---

## 8. Gold Analytics Layer

The Gold layer exposes business-ready analytical datasets optimized for dashboards, SQL analytics, and Genie.

Primary Gold tables:

```text
gold_flight_performance
gold_route_performance
gold_aircraft_performance
gold_booking_summary
```

These tables provide precomputed business metrics rather than exposing raw operational structures directly to downstream consumers.

---

### Flight Performance

Provides flight-level operational information including:

- flight status
- delay minutes
- delayed-flight indicator
- origin
- destination
- route
- aircraft

This table acts as a core analytical source for flight-level operational analysis.

---

### Route Performance

Aggregates operational performance by route.

Metrics include:

- total flights
- average delay
- delayed flights
- delay rate

Example:

```text
Route: AUH → HYD

Total Flights:     1,454
Average Delay:     15.58 minutes
Delayed Flights:   572
Delay Rate:        39.34%
```

---

### Aircraft Performance

Aggregates operational performance by aircraft.

Metrics include:

- total flights
- average delay
- delayed flights

Example:

```text
Aircraft:          AC014
Aircraft Type:     Airbus A350
Average Delay:     15.25 minutes
Total Flights:     832
Delayed Flights:   308
```

---

### Booking Summary

Aggregates passenger booking activity at flight level.

The dataset supports:

- flight-level booking analysis
- route demand analysis
- identification of high-demand flights
- cross-domain comparison between passenger demand and operational performance

---

## 9. Dimensional Analytics Model

A dimensional model was created to support analytical joins and semantic relationships.

Conceptually:

```text
                 dim_aircraft
                      │
                      ▼
dim_airport ───── fact_flights ───── booking data
                      │
                      ▼
                route analytics
```

Relationships were explicitly configured so analytical tools can understand how the core business entities relate.

This becomes particularly important for conversational analytics because a single business question may require information from multiple datasets.

---

## 10. Pipeline Orchestration

The complete data platform is orchestrated through a Databricks Job.

The end-to-end execution sequence is:

```text
01_generate_source_data
          │
          ▼
02_bronze_ingestion
          │
          ▼
03_silver_transformation
          │
          ▼
04_gold_analytics
```

The workflow combines source-data generation with Databricks Declarative Pipelines and executes the dependencies sequentially.

The broader logical flow is:

```text
Source Generation
       ↓
Bronze Ingestion
       ↓
Silver Transformation
       ↓
Data Quality / Quarantine
       ↓
Gold Analytics
       ↓
BI + AI Consumption
```

The complete job was executed end-to-end and Gold outputs were validated before being exposed to downstream analytical systems.

---

## 11. AI/BI Dashboard

A Databricks AI/BI Dashboard was created to provide operational visibility over the Gold analytical layer.

### Abu Dhabi Airline Operations Dashboard

The dashboard contains four headline KPIs:

```text
Total Flights          ~20K
Delayed Flights        6.87K
Average Delay          12.57 minutes
Total Bookings         ~30K
```

### Dashboard Visualizations

#### Flights by Route

Shows flight volume across the airline network.

#### Average Delay by Route

Highlights routes experiencing greater operational delays.

#### Aircraft Utilization by Total Flights

Compares the number of flights operated by individual aircraft.

#### Booking Demand by Route

Shows passenger booking demand across routes.

Together, these metrics provide an operational overview of network performance and passenger demand.

---

## 12. Genie Natural-Language Analytics

A Databricks Genie Agent was configured over the trusted Gold analytical layer.

The Genie configuration includes:

- Gold table sources
- business instructions
- curated example queries
- calculated measures
- calculated fields
- filters
- semantic relationships
- business terminology

This allows business users to interact with the airline data using natural language rather than manually writing SQL.

---

## 13. Genie Semantic Configuration

Several semantic components were configured to improve Genie's understanding of the business model.

### Measures

Reusable business measures were defined, including:

```text
Total Flights
Average Delay Minutes
```

These provide consistent metric definitions for natural-language analysis.

---

### Calculated Fields

A calculated route field was created to represent:

```text
origin → destination
```

This allows users to naturally ask questions at the route level.

---

### Relationships

Relationships were configured between analytical facts and dimensions, including:

```text
fact_flights → dim_aircraft
fact_flights → dim_airport
```

These relationships enable Genie to reason across related business entities.

---

### Curated SQL Examples

Representative analytical questions were supplied to teach Genie expected query patterns.

Examples include:

```text
Which aircraft operated the most flights?

Which route has the highest average delay?

Which flight has the most bookings?

Which routes have the highest average flight delays?
```

These examples help establish expected business semantics rather than relying solely on physical table and column names.

---

## 14. Genie Validation

The Genie Agent was validated against known Gold-layer results.

### Test 1 — Delayed Flights

Question:

```text
How many flights were delayed?
```

Result:

```text
6,873 delayed flights
```

**Status: PASS**

---

### Test 2 — Aircraft Delay Performance

Question:

```text
Which aircraft has the highest average delay?
```

Result:

```text
AC014 — Airbus A350

Average Delay:    15.25 minutes
Total Flights:    832
Delayed Flights:  308
```

**Status: PASS**

---

### Test 3 — Route Delay Performance

Question:

```text
Which route has the highest average delay?
```

Result:

```text
AUH → HYD

Average Delay:    15.58 minutes
Total Flights:    1,454
Delayed Flights:  572
Delay Rate:       39.34%
```

**Status: PASS**

---

### Test 4 — Highest Booking Demand by Flight

Question:

```text
Which flight has the most bookings?
```

Genie correctly identified a three-way tie:

```text
FL07723 — 16 bookings
FL00449 — 16 bookings
FL06600 — 16 bookings
```

**Status: PASS**

This test also exposed an important analytical consideration:

```sql
ORDER BY total_bookings DESC
LIMIT 1
```

would return only one row and therefore hide legitimate ties.

Removing the artificial single-row limitation allowed Genie to return the complete business answer.

---

### Test 5 — Highest Booking Demand by Route

Question:

```text
Which route has the highest number of bookings?
```

Result:

```text
AUH → JFK
Total Bookings: 4,688
```

Genie also identified the next highest-demand routes.

**Status: PASS**

---

## 15. Cross-Domain Business Insight Test

A more complex question was used to test Genie beyond simple aggregation:

```text
Which routes have both high booking demand
and high average delays?
```

Genie interpreted the business criteria as:

```text
High Booking Demand = ≥ 4,000 bookings
High Average Delay  = ≥ 12 minutes
```

Nine routes satisfied both conditions.

The highest-impact route identified was:

```text
AUH → HYD

Bookings:        4,342
Average Delay:   15.58 minutes
Total Flights:   1,454
Delayed Flights: 572
Delay Rate:      39.34%
```

Genie additionally generated a scatter visualization comparing:

```text
Total Bookings
       vs.
Average Delay
```

This demonstrates a significant capability of the solution:

> Operational performance and passenger-demand data can be analyzed together to identify business problems that would not be visible from either domain independently.

---

## 16. Key Engineering Decisions

### Bronze Preserves Source Data

Data-quality problems are intentionally not corrected during ingestion.

This preserves source fidelity, replayability, and traceability.

### Silver Owns Data Quality

Cleaning, validation, deduplication, and quarantine handling occur in the Silver layer.

This prevents data-quality logic from being distributed unpredictably across the platform.

### Bad Data Is Quarantined, Not Discarded

Rejected records remain available for investigation and remediation rather than disappearing from the pipeline.

### Gold Is Business-Oriented

Gold tables expose metrics aligned with business questions instead of raw source structures.

### Semantic Modeling Sits Above Physical Data

Genie is not simply pointed at arbitrary tables.

Measures, relationships, examples, terminology, and business rules are explicitly configured so natural-language questions resolve into meaningful analytics.

### BI and AI Consume the Same Trusted Gold Layer

```text
                    Gold Layer
                        │
                 ┌──────┴──────┐
                 ▼             ▼
          AI/BI Dashboard    Genie
                 │             │
                 └──────┬──────┘
                        ▼
                 Business Users
```

This reduces the risk of creating separate definitions of business metrics for traditional BI and conversational analytics.

---

## 17. End-to-End Platform Flow

The final platform can be summarized as:

```text
Raw Airline Data
       │
       ▼
Source Data Generation
       │
       ▼
Databricks Ingestion
       │
       ▼
Bronze Delta Tables
       │
       ▼
Silver Transformation
       │
       ├──── Invalid Records ────► Quarantine
       │
       ▼
Validated Silver Tables
       │
       ▼
Gold Business Transformations
       │
       ├── Flight Performance
       ├── Route Performance
       ├── Aircraft Performance
       └── Booking Summary
       │
       ├─────────────────────────┐
       ▼                         ▼
AI/BI Dashboard              Genie Agent
       │                         │
       ▼                         ▼
Visual Analytics        Natural-Language Analytics
       │                         │
       └────────────┬────────────┘
                    ▼
              Business Insights
```

---

## 18. Platform Evidence

The following screenshots provide implementation evidence for the major components of the platform.

### End-to-End Databricks Job

The complete workflow orchestrates source generation followed by Bronze, Silver, and Gold processing.

![Databricks Pipeline](docs/images/05_pipeline-graph.jpg)

The job demonstrates:

- source-data generation
- Bronze ingestion
- Silver transformation
- data-quality and quarantine processing
- Gold business transformations
- dependency-driven orchestration

---

### Airline Operations AI/BI Dashboard

The Gold analytical layer feeds the **Abu Dhabi Airline Operations Dashboard**.

![Airline Operations Dashboard](docs/images/06_airline-operations-dashboard.jpg)

The dashboard provides:

- Total Flights
- Delayed Flights
- Average Delay
- Total Bookings
- Flights by Route
- Average Delay by Route
- Aircraft Utilization
- Booking Demand by Route

This demonstrates the traditional BI consumption path of the Lakehouse.

---

### Genie Cross-Domain Analytics

The same trusted Gold layer is exposed through Databricks Genie for natural-language analytics.

Example business question:

> **Which routes have both high booking demand and high average delays?**

![Genie Cross-Domain Analysis](docs/images/07_genie-cross-domain-analysis.jpg)

Genie combined booking-demand and operational-performance information and identified routes satisfying both:

- **High booking demand:** ≥ 4,000 bookings
- **High average delay:** ≥ 12 minutes

The analysis identified **AUH → HYD** as a particularly significant route:

| Metric | Result |
|---|---:|
| Total Bookings | 4,342 |
| Average Delay | 15.58 min |
| Total Flights | 1,454 |
| Delayed Flights | 572 |
| Delay Rate | 39.34% |

Genie additionally generated a scatter visualization comparing **booking demand vs. average delay**, demonstrating analytical reasoning across multiple business domains.

---

### One Governed Data Foundation, Two Consumption Patterns

```text
                       Gold Layer
                           │
                ┌──────────┴──────────┐
                │                     │
                ▼                     ▼
        AI/BI Dashboard          Genie Agent
                │                     │
                ▼                     ▼
       Visual Analytics       Natural-Language
                                  Analytics
                │                     │
                └──────────┬──────────┘
                           ▼
                    Business Insights
```

Both consumption layers use the same trusted Gold data foundation, reducing the risk of inconsistent business metrics between dashboards and conversational analytics.

---

## 19. Business Value

The platform enables airline operations teams to move from:

```text
Raw Operational Data
        ↓
Trusted Lakehouse Data
        ↓
Operational KPIs
        ↓
Interactive Dashboards
        ↓
Natural-Language Analytics
        ↓
Cross-Domain Business Insights
```

A business user can move from a basic operational question:

> **How many flights were delayed?**

to a more decision-oriented question:

> **Which high-demand routes are also suffering from high delays?**

without manually joining datasets or writing SQL.

The architecture therefore supports two different analytical personas from the same governed data foundation:

**Analysts and operations teams**

```text
Gold → AI/BI Dashboard → Visual Analytics
```

**Business users and decision-makers**

```text
Gold → Genie → Natural-Language Analytics
```

---

## 20. Project Outcome

The completed solution demonstrates practical implementation of:

- Lakehouse architecture
- Medallion data modeling
- Delta Lake processing
- Apache Spark / PySpark transformations
- Databricks Declarative Pipelines
- data quality engineering
- quarantine patterns
- deterministic deduplication
- dimensional analytics
- workflow orchestration
- dependency-driven job execution
- Gold-layer business modeling
- Databricks AI/BI Dashboard development
- semantic modeling
- calculated measures and fields
- curated natural-language query patterns
- Databricks Genie
- cross-domain analytical reasoning
- Git-based source control

The result is an end-to-end airline analytics platform that transforms raw operational data into governed, business-ready information and exposes that information through both **traditional BI and AI-assisted conversational analytics** from a common trusted Lakehouse foundation.# Abu Dhabi Airline Lakehouse

An end-to-end airline data engineering and analytics platform built on Databricks.

The project demonstrates ingestion, Lakehouse architecture, declarative data pipelines, data quality and quarantine handling, Delta Lake transformations, dimensional analytics, orchestration, AI/BI dashboards, and natural-language analytics using Genie.

---
