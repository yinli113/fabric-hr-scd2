---
name: star-schema-modelling
description: Designs a Power BI-friendly star schema (facts/dimensions) from raw sources and SCD2 dimensions. Use when user asks about “star schema”, grain, conformed dimensions, key design, or which columns go in which Gold tables.
---
# TL;DR
This skill turns a messy HR dataset (or any daily-changing dataset) into a clean star schema with correct grain and join keys.

# Star Schema Design Skill

## Trigger scenarios
Use this skill when prompts include:
- star schema
- facts and dimensions
- grain (employee-day, event-day, etc.)
- SCD2 join keys
- Power BI model readiness

## Workflow
1. Identify the business grain(s):
   - example: employee-day fact, hierarchy edge fact, change event fact
2. Identify business keys and conformed dimensions:
   - example: `employee_id`, `date_key`, `status`, `team`, `org_unit`
3. Identify where data comes from in a medallion pipeline:
   - Bronze raw extracts (often “wide” and PII-heavy) -> Silver normalized tables -> Gold star schema
3. Choose SCD handling strategy:
   - SCD2 dimensions store versions with `effective_start_date`/`effective_end_date` and `is_current`
   - Facts join via `employee_sk` (resolved at the fact date)
4. Define each Gold table contract:
   - required keys
   - required descriptive columns
   - measures/flags and their definitions
5. Verify modeling correctness:
   - one-to-many relationships
   - no ambiguous joins
   - facts are additive at the chosen grain
6. Produce a short “BI mapping”:
   - what visuals/questions each table supports

## Output format
- A list of Gold tables with:
  - grain
  - primary keys
  - foreign keys
  - descriptive attributes
  - recommended Power BI usage (1 sentence each)
