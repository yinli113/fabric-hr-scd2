# Slowly Changing Dimension Type 2 (SCD2) Logic Explained

This document explains the PySpark logic used in `build_dim_employee_scd2()` to construct our Gold layer SCD2 table (`gold_dim_employee_scd2`).

## The Goal
In our raw data, we receive a "snapshot" of every employee, every single day. If an employee has worked here for 5 years, they have ~1,800 rows of redundant data. 

The goal of this function is to **compress** those daily snapshots into continuous, non-overlapping date ranges. For example: 
> *"John was an Analyst from 2021-01-01 to 2023-05-15, and a Senior Analyst from 2023-05-16 to 9999-12-31."*

Here is the step-by-step breakdown of how the PySpark code achieves this:

---

### Step 1: Create a "Fingerprint" for the data

```python
tracked_cols = [F.col(c).cast("string") for c in TRACKED_FIELDS]
tracked_hash = F.sha2(F.concat_ws("|", *tracked_cols), 256)
```

Instead of comparing 15 different columns (status, manager, salary band, address, etc.) one by one to see if something changed, we squash them all together into a single string separated by pipes `|` (e.g., `active|manager_1|marketing|...`) and generate a SHA-256 cryptographic hash (a "fingerprint"). 

If *anything* changes in those columns, the fingerprint completely changes. This makes change detection incredibly fast and reliable.

---

### Step 2: Detect when a change actually happened

```python
by_emp = Window.partitionBy("employee_id").orderBy("snapshot_date")

with_versions = (
    base.withColumn("prev_hash", F.lag("hash_diff").over(by_emp))
    .withColumn(
        "is_new_version",
        F.when(F.col("prev_hash").isNull() | (F.col("hash_diff") != F.col("prev_hash")), 1).otherwise(0),
    )
    # ...
)
```

We look at the employee's history in chronological order. We use a PySpark Window function (`F.lag`) to look at the **previous day's fingerprint**. 

If today's fingerprint is different from yesterday's fingerprint, it means the employee had a life event (moved houses, got promoted, changed managers). We flag that day with a `1` (`is_new_version`). If nothing changed, it gets a `0`.

---

### Step 3: Group the days into "Versions"

```python
    .withColumn("version_num", F.sum("is_new_version").over(by_emp))
```

By doing a running total (`F.sum`) of that `1` or `0` flag over time, we dynamically assign a `version_num` to blocks of time:
* Days 1 to 300 (no changes) might all sum up to `version 1`. 
* On day 301 they get promoted (flag = 1), so the running total increases, and days 301 to 600 become `version 2`.

---

### Step 4: Compress the daily rows into Start & End dates

```python
versions = with_versions.groupBy("employee_id", "version_num").agg(
    F.min("snapshot_date").alias("effective_start_date"),
    # ... other tracked fields ...
)
```

Now that we have grouped blocks of identical days into distinct "versions", we use `groupBy` to squish them down to a single row per version. We take the `min(snapshot_date)` of that version block, which becomes the exact date the promotion/change took effect (`effective_start_date`).

---

### Step 5: Figure out the End Date and Current Status

```python
    versions.withColumn("next_start_date", F.lead("effective_start_date").over(by_ver))
    .withColumn(
        "effective_end_date",
        F.when(F.col("next_start_date").isNull(), F.to_date(F.lit("9999-12-31"))).otherwise(
            F.date_sub(F.col("next_start_date"), 1)
        ),
    )
    .withColumn("is_current", F.when(F.col("next_start_date").isNull(), F.lit(True)).otherwise(F.lit(False)))
```

Finally, we need to know when a version ended. We use `F.lead` to peek at the **next version's** start date for that employee. 
* We subtract 1 day from the next start date to get the `effective_end_date` of the current row. 
* If there is no "next" version (meaning `next_start_date` is Null), it means this is their **current active record**. We set the end date to the year `9999-12-31` (a standard data warehousing trick) and set the `is_current` flag to `True`.

### Summary
This logic transforms massive, redundant daily snapshots into a clean, query-efficient history table. Tools like Power BI can now easily use this table to "travel back in time" to see the organization exactly as it looked on any specific date!

---

## How to Use this SCD2 Table in Power BI

When modeling SCD2 tables in Power BI, you generally do **not** connect the SCD2 dimension directly to your Fact tables. Doing so often creates circular dependencies ("Ambiguous Paths"). 

Instead, we use a **Snowflake Dimension** design pattern:

### 1. The "Static" Base Dimension
We create a central `gold_dim_employee_static` table. This table has exactly **one row per employee ID**. It acts as the "1" side of all relationships.

### 2. The Relationships (The "Spider Web")
* `gold_dim_employee_static` (1) ➡️ (Many) `gold_dim_employee_scd2`
* `gold_dim_employee_static` (1) ➡️ (Many) `gold_fact_employee_daily_snapshot`
* `gold_dim_employee_static` (1) ➡️ (Many) `gold_fact_employee_change_event`
* `gold_dim_employee_static` (1) ➡️ (Many) `gold_fact_reporting_line_daily`

### 3. How the Filters Flow
1. You drag `full_name_masked` or `gender` from the **Static** table to create your report slicers.
2. The user selects "John Smith".
3. The filter flows down the relationship line to the **SCD2** table, showing only John Smith's historical records.
4. It also flows down to the **Fact** tables to filter his daily headcount history and change events simultaneously!

This method bypasses the need for complex Surrogate Key (SK) point-in-time joins on massive fact tables, leveraging Power BI's DAX engine and relationship model for optimal performance.