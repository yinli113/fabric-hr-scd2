# TL;DR
This project generates realistic 3-month daily employee status changes and models them into an SCD2 employee dimension for Microsoft Fabric-style data engineering practice.

# Employee SCD2 Fabric Lab

## What this project does
- Generates synthetic employee HR data with realistic daily changes.
- Produces a single “wide” raw extract file (real-world capture) plus daily snapshots and event logs for practice.
- Builds `dim_employee_scd2` from snapshots.
- Builds `dim_employee_static` from base employee attributes.
- Validates core SCD2 integrity rules.

## Project structure
- `src/data_gen/generate_employee_data.py`: generates base employees, daily snapshots, and events.
- `src/data_gen/split_raw_extract_history_today.py`: splits wide raw extract into **history** (all days before latest) + **today** (latest `snapshot_date` only) for ADLS backfill + incremental demos.
- `src/scd2/build_scd2.py`: builds SCD2 dimension from daily snapshots.
- `src/scd2/build_static_dim.py`: builds static employee dimension.
- `data/raw/employee_hr_raw_extract.csv`: single wide raw extract file (Bronze-like capture) with PII + daily HR fields.
- `tests/test_scd2_integrity.py`: checks overlap, continuity, and current-row constraints.
- `.cursor/skills/employee-scd2-lab/SKILL.md`: reusable Agent Skill for this workflow.
- `src/fabric/nb_01_bronze.py`: Notebook 01 Bronze stage.
- `src/fabric/nb_02_silver.py`: Notebook 02 Silver stage.
- `src/fabric/nb_03_gold.py`: Notebook 03 Gold stage.
- `src/fabric/nb_04_tests.py`: Notebook 04 validation checks.

## Quick start
1. Generate data
   - `python3 src/data_gen/generate_employee_data.py`
2. (Optional) Split raw extract for history ingest vs one-day incremental
   - `python3 src/data_gen/split_raw_extract_history_today.py`
3. Build SCD2 dimension
   - `python3 src/scd2/build_scd2.py`
4. Build static dimension
   - `python3 src/scd2/build_static_dim.py`
5. Run validations
   - `python3 tests/test_scd2_integrity.py`

## Output files
- `data/raw/employee_hr_raw_extract.csv` (single wide raw extract capture)
- `data/raw/employee_hr_raw_extract_history.csv` / `data/adls_gen2/raw/employee_hr_raw_extract_history.csv` (all days **before** latest `snapshot_date`)
- `data/raw/employee_hr_raw_extract_today.csv` / `data/adls_gen2/raw/employee_hr_raw_extract_today.csv` (rows for **latest** `snapshot_date` only — incremental “today” slice)
- `data/adls_gen2/raw/employee_hr_raw_extract.csv` (ADLS Gen2 landing simulation; full extract if you did not split)
- `data/raw/employees_base.csv` (lab convenience; Silver normalization input)
- `data/raw/employee_daily_snapshot.csv` (lab convenience; Silver normalization input)
- `data/raw/employee_change_events.csv` (lab convenience; Silver normalization input)
- `data/processed/dim_employee_scd2.csv`
- `data/processed/dim_employee_static.csv`

## Fabric notebook runbook (end-to-end)
After you load `employee_hr_raw_extract_history.csv` into a Lakehouse `Files` path (or ADLS shortcut), run these scripts in a Fabric notebook:

1. Bronze notebook
   - `from src.fabric.nb_01_bronze import run_nb_01`
   - `run_nb_01(spark, source_path="Files/shortcut_connection_employee/employee_hr_raw_extract_history.csv", mode="copy", source_format="csv")`
2. Silver notebook
   - `from src.fabric.nb_02_silver import run_nb_02`
   - `run_nb_02(spark)`
3. Gold notebook
   - `from src.fabric.nb_03_gold import run_nb_03`
   - `run_nb_03(spark)`
4. Tests notebook
   - `from src.fabric.nb_04_tests import run_nb_04`
   - `run_nb_04(spark)`

Debug-friendly note:
- Default notebook settings now target a CSV file under `Files/shortcut_connection_employee/...` and use copy mode.
- If your shortcut points to Delta data (contains `_delta_log`), use `run_nb_01(..., mode="register", source_format="delta")`.

Recommended test notebook pattern:
- Keep transform notebooks focused on data building (`bronze`, `silver`, `gold`).
- Run data-quality/unit checks in a separate notebook (for example `nb_04_tests`) that calls `validate_gold_tables`.
- In Fabric pipelines, execute tests as a separate final step so failures are obvious and do not mix with transform logs.

Or run all stages in one call:
- `from src.fabric.run_pipeline import run_end_to_end`
- `run_end_to_end(spark, "Files/rawdev")`

Default table outputs:
- Silver: `silver_employee_daily_snapshot`, `silver_employees_base`, `silver_employee_change_events`
- Gold: `gold_dim_employee_static`, `gold_dim_employee_scd2`, `gold_fact_employee_daily_snapshot`, `gold_fact_reporting_line_daily`, `gold_fact_employee_change_event`

SCD2 tracked attributes now include operational fields that can change over time:
- `org_unit`, `role_family`, `location`
- `phone_number`
- `home_address_line1`, `home_address_city`, `home_address_state`, `home_address_postal_code`
