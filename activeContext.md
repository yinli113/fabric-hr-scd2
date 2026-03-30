# TL;DR
Fabric notebook-ready pipeline code is now added for Bronze -> Silver -> Gold, including deterministic masking, SCD2 build logic, fact table creation, and Gold validation checks.

# Active Context

## Current phase
Fabric execution enablement for end-to-end medallion flow.

## Completed
- Project scaffold
- Agent Skill v1
- Data generator script (demographics + mutable location/role family)
- Static dimension builder script
- SCD2 builder script (Type 2 for historized HR attributes)
- Integrity checks
- Fabric docs starter
- Fabric Spark scripts for Bronze -> Silver
- Fabric Spark scripts for Silver -> Gold (SCD2 + facts)
- Fabric Spark validation script for Gold integrity
- README runbook for Fabric notebook execution

## Next
- Execute scripts in Fabric workspace against OneLake path
- Add daily incremental pattern for `employee_hr_raw_extract_today.csv`
- Wire notebook execution into a Fabric pipeline schedule
- Add Power BI semantic model on Gold tables
