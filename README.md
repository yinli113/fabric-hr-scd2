# Global Workforce & Org Dynamics (Fabric HR SCD2 Lab)

![Org Explorer Dashboard](images/employee_org_explorer.png)

## Overview
This project generates realistic daily employee status changes and models them into an Enterprise-grade Slowly Changing Dimension Type 2 (SCD2) for Microsoft Fabric. It demonstrates an end-to-end modern Data Engineering and Business Intelligence workflow, from raw CSV ingestion to dynamic, time-traveling Power BI dashboards.

## Project Architecture

![Employee Pipeline](images/employee_pipeline.png)

### The Medallion Data Model
The project strictly follows the Databricks/Fabric Medallion Architecture:
- **Bronze (Ingestion):** Lands raw, daily HR CSV extracts via OneLake shortcuts or direct upload. Records metadata like ingestion timestamps.
- **Silver (Conformed & Cleansed):** Deduplicates records, computes MD5/SHA-256 hashes to detect row-level changes, and applies deterministic tokenization/masking to sensitive PII (Emails, Tax IDs).
- **Gold (Curated & Modeled):** Transforms daily snapshots into a robust Star Schema. Features a `gold_dim_employee_scd2` dimension tracking precise `effective_start_date` and `effective_end_date` bounds, allowing accurate historical reporting without overlapping timelines.

### The CI/CD & Deployment Strategy

![Workspace Architecture](images/workspace_architecture.png)

We utilize a native Fabric ALM (Application Lifecycle Management) approach:
- **Dev & Prod Isolation:** Dedicated `Employee_Dev_WS` and `Employee_Prod_WS` workspaces.
- **Deployment Pipelines:** 1-click promotion of Notebooks, Lakehouses, and Reports from Dev to Prod.
- **Automated Orchestration:** Fabric Data Factory pipelines triggered by ADLS Gen2 Blob Storage events run the PySpark notebooks (`Bronze ➡️ Silver ➡️ Gold ➡️ Tests`) automatically upon new data arrival.

## AI-Driven Development Approach

This entire project was architected and built using advanced **AI Agents** (Cursor/Codex). Here is how the AI ecosystem powered the development:

1. **Agent Skills:** We utilized predefined `.cursor/skills` (e.g., `fabric-etl-silver-gold`, `medallion-architecture-fabric`, `deterministic-pii-masking`) to give the AI instant domain expertise. Instead of prompting the AI with basic Python instructions, the AI read these skill files to understand enterprise Fabric design patterns before writing a single line of code.
2. **Custom Rules:** Workspace rules controlled the AI's editing behavior, forcing it to follow a specific directory structure (`src/fabric/`, `tests/`) and prioritize in-place file editing over creating duplicate scripts.
3. **Subagents & Tool Calling:** The AI autonomously executed terminal commands, performed Python debugging, explored the local file system to auto-discover CSV schemas, and formatted Markdown documentation.
4. **Iterative Problem Solving:** When Fabric-specific errors occurred (e.g., `[SCHEMA_NOT_FOUND]` or `HTTP 430 Compute Limits`), the AI diagnosed the cloud infrastructure issues and provided immediate UI troubleshooting steps alongside the PySpark code fixes.

## Project Structure
- `src/data_gen/`: Scripts to generate synthetic HR data and daily snapshots.
- `src/fabric/`: The core Medallion PySpark notebooks (`nb_01_bronze.py` through `nb_04_tests.py`).
- `src/scd2/` & `src/security/`: Core transformation and PII masking logic.
- `docs/` & `*.md`: Extensive documentation on SCD2 logic, Power BI DAX, and CI/CD.
- `images/`: Architecture diagrams and dashboard screenshots.

## Quick Start
1. **Generate Data:** 
   `python3 src/data_gen/generate_employee_data.py`
2. **Fabric Ingestion:** Upload the generated `employee_hr_raw_extract_history.csv` to your Fabric Lakehouse.
3. **Run Pipeline:** Execute notebooks 01 through 04 in your Fabric workspace to build the Gold Star Schema.
4. **Power BI:** Connect a new report to the SQL Analytics Endpoint, wire the Snowflake schema, and build the dynamic org chart using the DAX measures provided in `POWERBI_DASHBOARD.md`.

---
*For a deep dive into the SCD2 logic, see [SCD2_README.md](SCD2_README.md). For dashboard DAX, see [POWERBI_DASHBOARD.md](POWERBI_DASHBOARD.md).*
