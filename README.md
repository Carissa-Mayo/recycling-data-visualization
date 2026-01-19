# California RDRS Waste System Baseline Dashboard (2020–2025)

This project builds a small, end-to-end reporting pipeline from CalRecycle RDRS exports to a Power BI dashboard.

**Goal:** consolidate multiple RDRS report exports into a consistent dataset, run basic data checks, create a simple SQL reporting layer (“mart”), and publish an executive-style dashboard showing baseline levels and trends.

---

## What’s in the data

Source data are CalRecycle RDRS quarterly exports (2020–2025):

- **Report 8:** statewide totals (California)
- **Report 1:** jurisdiction totals
- **Report 3:** facility totals by material stream

**Important:** these reports have different “grains” (statewide vs jurisdiction vs facility). In the dashboard, I keep pages scoped to a single source to avoid mixing incompatible totals.

---

## Output tables (standardized schema)

All source files are reshaped into a consistent long format:

| column        | meaning |
|--------------|---------|
| year         | integer year |
| quarter      | 1–4 |
| yearquarter  | `YYYY-Q#` |
| source       | which report export it came from |
| entity       | jurisdiction / facility / California |
| stream       | category/stream label (varies by report) |
| tons         | numeric value |

---

## Pipeline

1) Extract: Excel -> standardized CSV

Reads each report and reshapes: src/01_extract_to_csv.py
outputs:
- data_intermediate/rdrs_1_long.csv
- data_intermediate/rdrs_8_long.csv
- data_intermediate/rdrs_3_long.csv

2) Load: CSV -> Postgres staging

Loads the standardized CSVs into staging.rdrs_long

3) Build mart tables in SQL

4) Run in DBeaver

- sql/01_create_schemas_tables.sql
- sql/02_build_mart.sql

5) Export from Postgres for PowerBI


## Next Improvements
* Stream mapping table (short labels + categories)
* Automated refresh (cloud DB or scheduled file refresh)
* KPI tables in SQL (latest quarter, QoQ %, trailing 4Q) for Power BI Service cards
