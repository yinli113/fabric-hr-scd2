---
name: fabric-powerbi-cicd
description: Plans and documents end-to-end CI/CD for Microsoft Fabric and Power BI, including dev/staging/prod workspaces and deployment pipelines. Use when user asks about Fabric CICD, Power BI deployment pipelines, Git integration, PBIP, or release automation.
---
# TL;DR
This skill helps set up a dev->staging->prod pipeline for Fabric Lakehouse/ETL artifacts and Power BI PBIP reports with automated checks.

# Fabric + Power BI CI/CD Skill

## Trigger scenarios
Use when prompts include:
- CI/CD
- deployment pipelines
- dev staging prod
- Power BI PBIP
- Git integration
- “release to production”

## Workflow
1. Create environment workspaces:
   - `dev` for active development
   - `staging` for QA validation
   - `prod` for business consumption
2. Use Git integration for version control of artifacts (PBIP + code-first assets).
3. Define Fabric deployment pipelines:
   - deploy Gold tables/semantics before Power BI datasets/reports
   - set pipeline dependency order
4. Add quality gates:
   - unit tests for SCD2 integrity
   - minimal data validation (row counts, null keys, date continuity)
   - schema contract checks (expected columns/grains exist)
5. Automate promotions:
   - run tests on merge into main (or release branch)
   - deploy dev->staging, then staging->prod after approval gates

## Output format
- A short “promotion checklist” (what deploys first)
- A dependency graph description (items and ordering)
- A recommended test gate list
