---
name: medallion-architecture-fabric
description: Guides medallion (Bronze/Silver/Gold) architecture implementation for Microsoft Fabric, including where security, masking, and data contracts live. Use when user asks about “medallion architecture”, “Lakehouse layers”, or how to structure ETL steps in Fabric.
---
# TL;DR
This skill helps structure an end-to-end Fabric lakehouse pipeline with clear layer responsibilities and promotion-safe contracts.

# Medallion Architecture Skill (Fabric)

## Trigger scenarios
Use this skill when prompts include:
- medallion
- bronze silver gold
- Fabric lakehouse
- “what goes where”
- data contracts

## Workflow
1. Start from business goals and choose analytics-ready targets:
   - hierarchy as-of
   - trends (headcount/leave/mobility)
   - security posture
2. Assign responsibilities to each layer:
   - Bronze: raw ingest, minimally transformed, restricted access for PII
     - In real environments: raw landing happens in ADLS Gen2, and Fabric reads it via Lakehouse shortcuts into OneLake.
   - Silver: schema standardization, deduping, type enforcement, change capture shaping
   - Gold: curated dimensions/facts (SCD2 + star schema), join-ready outputs
3. Define security boundaries early:
   - keep unmasked PII restricted to Bronze
   - create masked/tokenized analytics columns for Silver/Gold
4. Define contracts:
   - table grain
   - primary keys / join keys
   - validity rules (SCD2 ranges, non-overlap)
5. Produce an ETL execution order (dependency graph):
   - Silver before Gold
   - Gold dimensions before Gold facts
   - Gold before Power BI dataset build

## Output format
Provide:
- a short layer responsibility map
- a dependency-ordered list of pipeline steps
- a “contract checklist” per Gold table
