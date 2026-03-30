# CI/CD in Microsoft Fabric: Deployment Pipelines

Even without external tools like GitHub Actions or Azure DevOps, this project implements a robust **Continuous Integration and Continuous Deployment (CI/CD)** lifecycle using Fabric's native Application Lifecycle Management (ALM) features.

This guide outlines how we promote our Medallion Architecture (PySpark Notebooks, Data Pipelines, and Power BI Reports) from Development to Production safely.

## 1. The Multi-Workspace Strategy
We physically separate our environments to ensure development work never impacts the live dashboard.
* **`Employee_Dev_WS`:** The sandbox. This is where data engineers write PySpark code, test new SCD2 logic, and build experimental Power BI visuals against a `rawdev` data source.
* **`Employee_Prod_WS`:** The live environment. This workspace is locked down. It connects to the `rawprod` data source and serves the finalized Power BI dashboard to business users.

## 2. Fabric Deployment Pipelines
Instead of manually copying code or exporting/importing `.ipynb` files, we use **Fabric Deployment Pipelines** to automate the release process.

1. **Create the Pipeline:** We created a 2-stage Deployment Pipeline (`Dev ➡️ Prod`).
2. **Assign Workspaces:** The `Employee_Dev_WS` is assigned to the Dev stage, and `Employee_Prod_WS` is assigned to the Prod stage.
3. **Compare & Diff:** Before deploying, Fabric automatically scans both workspaces and highlights exactly which artifacts (Notebooks, Lakehouses, Semantic Models, or Reports) have been modified, added, or deleted.

## 3. The Release Workflow
When a new feature (like our incremental `append` logic for the Bronze layer) is ready for production, the workflow is:

1. **Automated Testing:** In the Dev workspace, we run `nb_04_tests.py` to ensure the new logic hasn't created overlapping SCD2 dates or data quality issues.
2. **Review:** The team reviews the "Compare" screen in the Deployment Pipeline to verify only the intended notebooks are being promoted.
3. **Deploy:** A single click pushes the updated code from Dev into the Prod workspace.
4. **Rebinding:** Fabric automatically updates the connections so the newly promoted Prod notebooks talk to the Prod Lakehouse instead of the Dev Lakehouse.

## 4. Automated Orchestration (The "Live" Pipeline)
Once the code is in Production, we use a **Fabric Data Factory Pipeline** to run the ELT process continuously:
* We chain the notebooks together: `01_Bronze ➡️ 02_Silver ➡️ 03_Gold ➡️ 04_Tests`.
* We attach a **Storage Event Trigger** so that the moment a new daily HR snapshot CSV lands in the `rawprod` ADLS container, the pipeline automatically wakes up, ingests the data, recalculates the SCD2 history, and refreshes the Power BI dashboard without human intervention.
