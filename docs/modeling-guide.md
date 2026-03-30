# TL;DR
This guide explains how raw daily employee snapshots become an SCD2 employee dimension for analytics.

# Modeling Guide

## Source tables
- `employees_base`: stable employee attributes.
- `employee_daily_snapshot`: daily mutable employee state.
- `employee_change_events`: attribute-level change history.

## SCD2 target
- `dim_employee_scd2`
- keys: `employee_sk` (surrogate), `employee_id` (business key)
- validity: `effective_start_date`, `effective_end_date`, `is_current`
- change fingerprint: `hash_diff`

## Core rules
- Create a new row when tracked attributes change.
- Close previous row at `new_start_date - 1 day`.
- Exactly one current row per employee.

## How Gold supports the HR business goals
- **Hierarchy as-of date**: build reporting-line edges by joining the daily snapshot’s `manager_id` to the manager’s SCD2 version valid on that date.
- **Trends**: build employee-day facts at `date_key` grain (headcount flags, status, org/team, etc.) and aggregate in Power BI.
- **Mobility**: historized attributes in `dim_employee_scd2` (org/team/role_family/location changes) let you analyze movement over time.
- **Lifecycle health**: derive KPIs from status sequences (leave durations, termination/rehire windows, etc.).

## Raw extract shape (real-world capture pattern)
In many enterprise sources, the raw capture arrives as a single “wide” extract (often including both identity/PII and the latest daily attributes).
For this lab:
- Bronze/raw can be represented by `employee_hr_raw_extract.csv` (wide extract).
- Silver normalizes it into snapshots/events and builds clean inputs for Gold SCD2 + star-schema facts.

## Sensitive data modeling (how to avoid leaking PII)
A practical approach for star-schema + SCD2 labs:
- **Demographics and identity** (DOB, gender, name, government IDs) are usually **low-frequency**.
  - Keep them in a Type 0/Type 1 style dimension such as `dim_employee_static`.
  - Mask or tokenize PII fields in the analytics-facing layer if the demo needs it.
- **HR operational attributes** (status, salary band, team/org assignment, work mode, manager) are historized.
  - Put them in `dim_employee_scd2` with effective dating (Type 2).
- **Daily snapshot facts** should join by `employee_sk` and avoid raw PII columns.

Deterministic masking note:
- When you mask/tokenize deterministically, `employee_id`-based joins remain stable for BI and validation queries.
