# How We Built This: The Data Engineering Behind the Dashboards

To achieve dynamic, "time-traveling" HR dashboards, a Data Engineer must move far beyond standard data imports. The secret lies in strict Medallion Architecture and advanced Slowly Changing Dimension (SCD2) modeling.

**1. Medallion Architecture (Bronze ➡️ Silver ➡️ Gold)**
We built a structured pipeline. The **Bronze** layer ingested raw CSV snapshots exactly as they arrived. In the **Silver** layer, we cleaned the data, deduplicated records, and applied deterministic masking to sensitive PII (like emails and tax IDs). This ensures analysts have secure data without losing referential integrity.

**2. The SCD2 Data Model**
The magic happens in the **Gold** layer. Instead of keeping a billion daily snapshot rows, we wrote PySpark Window functions to detect exactly when an employee's profile changed (e.g., a promotion or move). We compressed these changes into an **SCD2 Table** with precise `effective_start_date` and `effective_end_date` columns. 

**3. The Snowflake Pattern for Power BI**
To avoid circular relationship errors in Power BI, we split the dimensions. We created a `gold_dim_employee_static` table (one row per person) to act as the core filter, which flows outwards to the SCD2 history table and the daily fact tables. This allows a user to select an employee and instantly see their historical states without complex Surrogate Key joins.

**4. Why Microsoft Fabric?**
Fabric made this seamless. Traditionally, this requires stitching together Azure Data Factory, Databricks, and a dedicated SQL Server. In Fabric, we uploaded a file to OneLake, wrote PySpark in a notebook, and immediately flipped a switch to the "SQL Analytics Endpoint." The Delta tables we created in Spark were instantly available for semantic modeling and direct DAX querying in the Power BI web editor—zero data movement required.

**5. Fabric-Native CI/CD**
We utilized a multi-workspace strategy (Dev and Prod). Using Fabric Deployment Pipelines, we instituted a safe, 1-click promotion process from Dev to Prod. Finally, we linked our notebooks in a Data Factory Pipeline with a Storage Event Trigger, achieving a fully automated, event-driven CI/CD workflow without needing external orchestration tools.