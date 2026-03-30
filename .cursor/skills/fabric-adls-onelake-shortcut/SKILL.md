---
name: fabric-adls-onelake-shortcut
description: Documents and guides the ADLS Gen2 landing pattern into Fabric Lakehouse via OneLake shortcuts. Use when user asks about "shortcut", "ADLS Gen2 integration", or how to structure raw ingest for medallion pipelines.
---
# TL;DR
This skill helps you design a production-like raw landing flow: ADLS Gen2 -> Lakehouse shortcut -> Bronze -> Silver -> Gold.

# ADLS Gen2 -> OneLake Shortcut Skill (Fabric)

## Trigger scenarios
Use this skill when prompts include:
- ADLS Gen2
- OneLake shortcut
- Lakehouse shortcut
- raw landing pattern
- Bronze ingest from external storage

## Workflow
1. Define the raw extract contract:
   - file format (CSV/Parquet)
   - naming/partition convention
   - schema stability expectations
2. Land raw extracts in ADLS Gen2:
   - a single wide extract file is common for source-system captures
3. Create a Fabric Lakehouse shortcut:
   - point Bronze/source read to the ADLS path
4. Standardize in Silver:
   - enforce schema
   - apply deterministic masking/tokenization rules
   - normalize wide extracts into snapshots/events tables
5. Curate in Gold:
   - SCD2 dimensions (effective dating, non-overlap, single current)
   - star schema facts/dimensions for Power BI

## Output format
- a short checklist of items to configure in Fabric
- an ETL dependency ordering list (raw -> silver -> gold -> Power BI)
