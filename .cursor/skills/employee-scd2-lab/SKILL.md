---
name: employee-scd2-lab
description: Builds and validates realistic employee HR history for star schema + SCD2 modeling, including deterministic masking for sensitive attributes and guidance for Fabric Gold tables. Use when user asks about employee datasets, daily status changes, org hierarchy as-of a date, SCD2 dimensions, medallion architecture, ETL, or Power BI-ready modeling.
---
# TL;DR
This skill standardizes how to generate realistic employee daily changes, transform them into SCD2 dimensions, validate integrity, and produce Fabric Gold-ready tables for HR analytics (including hierarchy-by-date modeling).

# Employee SCD2 Lab Skill

## Trigger scenarios
Use this skill when prompts include:
- employee dataset
- SCD2
- daily snapshots
- status history
- Microsoft Fabric HR modeling

## Workflow
1. Generate base employees, synthetic sensitive attributes (optional), and 3-month daily snapshots.
   - Prefer a single raw extract capture pattern:
     - store one wide HR extract in Bronze (e.g., `employee_hr_raw_extract.csv`)
     - derive normalized snapshots/events in Silver
2. Record attribute change events for tracked fields (`status`, `manager_id`, `team`, `salary_band`, `fte_flag`, `work_mode`, `org_unit`, `role_family`, `location`).
3. Build `dim_employee_static`:
   - keep low-frequency HR identity attributes
   - apply deterministic masking/tokenization for any PII included in raw
4. Build `dim_employee_scd2`:
   - historize HR operational attributes using change detection (Type 2)
5. Build Gold-ready HR tables (depending on BI needs):
   - `fact_employee_daily_snapshot` (employee-day grain facts for headcount/status/workforce mix)
   - `fact_reporting_line_daily` (child->manager edges by `date_key` for org hierarchy as-of date)
   - `fact_employee_change_event` (event facts from `employee_change_events`)
6. Validate:
   - no overlap per employee
   - max one `is_current=true` row per employee
   - contiguous effective dates
   - optional: hierarchy consistency (manager edge resolves to a valid manager SCD2 version for that day)
7. Summarize outcomes and assumptions in project docs (contracts for Bronze/Silver/Gold columns and grains).

## Output checklist
- [ ] `data/raw/employees_base.csv`
- [ ] `data/raw/employee_daily_snapshot.csv`
- [ ] `data/raw/employee_change_events.csv`
- [ ] (If added) raw PII columns exist in Bronze with restricted access policy
- [ ] `data/raw/employee_hr_raw_extract.csv` (single wide raw extract capture)
- [ ] `data/processed/dim_employee_static.csv`
- [ ] `data/processed/dim_employee_scd2.csv`
- [ ] (If built) `fact_employee_daily_snapshot`
- [ ] (If built) `fact_reporting_line_daily`
- [ ] (If built) `fact_employee_change_event`
- [ ] Validation script passes

## Failure handling
- If overlap is detected, inspect effective date assignment logic.
- If multiple current rows exist, ensure previous row closes before new row starts.
- If no events exist, increase mutation probabilities in generator config.
