# TL;DR
Use this story to present your Fabric + SCD2 project as business-ready and technically grounded for HR and hiring managers.

# Portfolio Story

## Problem
Most tutorial datasets are static and do not reflect real employee lifecycle changes.

## Solution
Built a synthetic HR dataset with daily status transitions and modeled it with SCD2 to preserve history.

## Business value
- Enables accurate historical reporting instead of point-in-time overwrites.
- Demonstrates real-world data engineering patterns used in enterprise HR analytics.
- Supports leader-ready questions such as org structure as-of a date, workforce trends, and mobility/leave analytics.

## Technical highlights
- Daily snapshots + event logs.
- Hash-based SCD2 change detection.
- Integrity checks for overlap, continuity, and single-current-row constraints.

## Security practice (PII + masking)
This lab also supports a realistic security workflow:
- Keep sensitive fields in the raw layer under restricted access.
- Promote only masked/tokenized versions of PII into the analytics layer.
- Prefer deterministic masking so the same employee keeps the same masked identifiers across time.

## Why this project matches current job-market expectations
This project is structured like a real enterprise analytics delivery:
- Medallion layers with explicit Bronze/Silver/Gold responsibilities and data contracts.
- ADLS Gen2 raw landing pattern (simulated here) plus Lakehouse shortcut into OneLake.
- Power BI-ready star schema and a hierarchy-by-date BI use case.
- Deterministic PII masking so joins remain stable while raw PII stays restricted.
- CI-like validation gates (SCD2 integrity tests) before promoting Gold outputs.
