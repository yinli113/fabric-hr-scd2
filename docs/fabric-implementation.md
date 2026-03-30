# TL;DR
This project maps cleanly to a Fabric medallion flow: raw snapshots/events in bronze, cleaned history in silver, SCD2 dimensions in gold.

# Fabric Implementation

## Lakehouse layers
- Bronze: raw CSV ingests from generator.
- Silver: standardized types and deduplicated records.
- Gold: `dim_employee_scd2` for BI and downstream marts.

## Suggested pipeline
1. Notebook or Dataflow ingests raw files.
2. Transformation step computes hash-based change detection.
3. Gold table upsert applies SCD2 close-and-open logic.

## Notebooks vs Dataflows Gen2 (what to use here)
- Use **Notebooks** for the “hard parts”:
  - SCD2 change detection + effective dating
  - resolving manager hierarchy “as-of date”
  - building Gold facts/dimensions for star schema
- Use **Dataflows Gen2** for the “easy parts”:
  - lightweight schema enforcement, basic parsing, deduping
  - standardization steps before your Gold transformations

## HR business goals for the Gold layer (what leaders want)
1. **Org structure “as-of date”**
   - Show the reporting hierarchy on a selected day.
   - Support span-of-control (direct reports per manager) and manager drill-down.
2. **Headcount + workforce mix trends**
   - Track headcount over time by `org_unit`, `team`, and `status`.
   - Track workforce mix by `work_mode` and `fte_flag`.
3. **Mobility and internal movement**
   - Track promotions and salary band movement over time.
   - Track transfers across `org_unit`/`team` and location moves (historized).
4. **Lifecycle health (leave, attrition, rehire patterns)**
   - Analyze leave-to-return and churn patterns (leave durations, return likelihood).
   - Analyze termination and rehire behavior over time.
5. **Governance and security posture**
   - Ensure sensitive PII is restricted in raw/bronze.
   - Promote only masked/tokenized PII into silver/gold/semantic models.

## Suggested Gold tables (starter)
- `dim_date` (date slicer + time attributes)
- `dim_employee_static` (Type 0/1 identity and masked PII where needed)
- `dim_employee_scd2` (Type 2 historized HR attributes)
- `fact_employee_daily_snapshot` (employee-day grain for headcount and workforce mix trends)
- `fact_reporting_line_daily` (child->manager edges by `date_key` for hierarchy-as-of visual)
- `fact_employee_change_event` (event-day grain for explaining “what changed”)

## Medallion business goals (why this ETL shape)
The purpose of medallion (Bronze/Silver/Gold) is to align engineering work with business outcomes:
- **Traceability**: Bronze keeps an auditable, reproducible raw ingest snapshot.
- **Quality + conformance**: Silver standardizes schema, types, deduping, and change-capture rules.
- **BI performance and stability**: Gold exposes a curated, join-ready layer with consistent business definitions.
- **Change isolation**: breaking changes in raw sources stay contained; downstream models only depend on Silver/Gold contracts.
- **Security boundaries**: sensitive data can remain more restricted in Bronze, while masked/approved fields are promoted to Gold.

## Sensitive data practice (deterministic masking)
To practice security for HR datasets, a common pattern is:
- Keep **PII in the raw layer** (Bronze) but restrict access using workspace/lakehouse permissions and RBAC.
- Create **analytics-ready masked columns** in Silver/Gold.
- Use **deterministic masking/tokenization** so joins and aggregations remain stable:
  - Email/phone: masked value stays the same for a given employee across time.
  - IDs: store only `*_last4` (or token/hash).
  - Addresses: keep coarse attributes (country/state/city) and mask street/postal.

For this project, you would typically:
- Put DOB/name/gender/tax identifiers into `dim_employee_static` and mask there if needed.
- Ensure `employee_daily_snapshot` (and anything in Gold) does not expose raw PII columns.

## End-to-end Fabric pipeline (Dev -> Staging -> Prod + Power BI + CI/CD)
High-level workflow:
1. **Create three Fabric workspaces**: `dev`, `staging`, `prod` (each with the right permissions).
2. **Use Git integration** for the workspace that hosts your development artifacts (Lakehouse/Notebooks/Pipelines/Power BI PBIP).
3. **Create deployment pipelines** in Fabric to promote items across environments:
   - `dev -> staging -> prod`
   - Configure pipeline dependency ordering (Gold tables before Power BI datasets, etc.).
4. **Build Gold models and semantic layer**:
   - Gold tables: `dim_employee_static`, `dim_employee_scd2`, and any facts (daily snapshot and/or events).
   - Power BI: connect to Gold via Direct Lake/Import and define measures.
5. **CI/CD (recommended checks before promotion)**:
   - Run unit tests for SCD2 integrity (`tests/test_scd2_integrity.py`).
   - Run a small data validation suite (row counts, date continuity, no null keys).
   - Only deploy to `staging/prod` when tests pass.

This matches how teams maintain contracts: development is flexible, but staging/prod are controlled by tested deployments.

## Real-world raw landing: ADLS Gen2 -> OneLake shortcut
In production, raw extracts typically land as files/partitions in **ADLS Gen2** and Fabric reads them via **Lakehouse shortcuts** into OneLake.

Recommended pattern:
1. Land the raw extract (example: a single wide HR extract) into ADLS Gen2 (Bronze/source location).
2. In Fabric, create a **shortcut** from the Lakehouse Bronze folder to the ADLS path.
3. Transform in **Silver** (standardize schema, apply deterministic masking, deduplicate, change-capture shaping).
4. Build **Gold** (SCD2 dimensions + star-schema facts) from Silver contracts.

In this lab:
- `data/adls_gen2/raw/employee_hr_raw_extract.csv` simulates the ADLS landing.
- `data/raw/employee_hr_raw_extract.csv` is the local representation of the Bronze capture.
