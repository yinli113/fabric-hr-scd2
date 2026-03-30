---
name: fabric-etl-silver-gold
description: Creates an ETL plan for Fabric Silver->Gold transformations including SCD2 close/open logic, deduping, and deterministic change detection. Use when user asks about Fabric ETL steps, “how to build Gold tables”, or SCD2 implementation in a lakehouse.
---
# TL;DR
This skill provides an implementation workflow for building Silver and Gold tables in Microsoft Fabric with SCD2 correctness checks.

# Fabric Silver->Gold ETL Skill

## Trigger scenarios
Use this skill when prompts include:
- Fabric ETL
- Silver Gold
- upsert
- SCD2 implementation
- “close-and-open”

## Workflow
1. Define inputs and their grains:
   - Bronze raw extracts (often wide/PII-heavy)
   - Silver normalized snapshots/events (derived)
   - which columns are tracked for SCD2
2. Build Silver:
   - enforce schema and types
   - remove duplicates
   - keep deterministic timestamps and keys
3. Build Gold dimensions (SCD2):
   - compute change fingerprint (hash_diff or equivalent)
   - close previous version at `start_date - 1 day`
   - open new current version with `is_current=true`
4. Build Gold facts:
   - employee-day facts join to `dim_employee_scd2` as-of each date
   - hierarchy edge facts join child->manager SCD2 versions as-of that date
5. Validation and test loop:
   - no overlap and exactly one current version per business key
   - effective date continuity
   - referential integrity for fact joins

## Output format
- Implementation steps in execution order
- A short “test plan” checklist for each target Gold table
