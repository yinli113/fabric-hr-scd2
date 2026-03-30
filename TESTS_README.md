# Gold Layer Data Quality Tests Explained

This document explains the PySpark testing logic used in `nb_04_tests.py` to validate our Gold layer tables, specifically focusing on the Slowly Changing Dimension Type 2 (SCD2) table.

## Why Do We Need These Tests?
SCD2 tables are notoriously tricky to get perfectly right. A single logic error during transformation could cause an employee to appear twice on the same date, or completely disappear from the reports for a week.

This testing notebook acts as an automated "Data Quality Gate." If any of these checks fail, it raises an `AssertionError` preventing bad data from reaching Power BI.

Here is a step-by-step breakdown of the checks:

---

### 1. The "Null Key" Check

```python
null_key_cnt = scd.filter(
    F.col("employee_id").isNull() | F.col("effective_start_date").isNull() | F.col("effective_end_date").isNull()
).count()
```

**What it does:** Scans the `gold_dim_employee_scd2` table to ensure that every single row has a valid `employee_id`, `effective_start_date`, and `effective_end_date`.
**Why it matters:** If any of these core fields are null, table joins in Power BI will break. You cannot filter a report by a date range if the date is missing.

---

### 2. The "Overlap" Check (Crucial for SCD2)

```python
by_emp = Window.partitionBy("employee_id").orderBy("effective_start_date")
overlap_cnt = (
    scd.withColumn("next_start", F.lead("effective_start_date").over(by_emp))
    .filter(F.col("next_start").isNotNull() & (F.col("effective_end_date") >= F.col("next_start")))
    .count()
)
```

**What it does:** Looks at a specific row's `end_date` and compares it to the *next* chronological row's `start_date` for that same employee. It triggers a failure if the current `end_date` is greater than or equal to the next `start_date`.
**Why it matters:** An employee cannot have two active profiles on the exact same day. If row 1 says "Analyst from Jan 1 to **Jan 5**" and row 2 says "Senior Analyst from **Jan 5** to Dec 31", there is an overlap on Jan 5th. Power BI would double-count this employee's salary and headcount on that day. 

---

### 3. The "One Current Row" Check

```python
current_cnt = (
    scd.filter(F.col("is_current") == True)
    .groupBy("employee_id")
    .count()
    .filter(F.col("count") != 1)
    .count()
)
```

**What it does:** Filters the table to only show rows where `is_current = True`, then groups by employee. If any employee has `0` current rows or `2+` current rows, the test fails.
**Why it matters:** Every person who has ever existed in the dataset must have exactly **one** "current" state in the dimension table (even if they are terminated, their current status is simply "terminated"). If they have two current rows, they are double-counted today. If they have zero, they disappear from today's active reporting.

---

### 4. The "Continuity / Gap" Check

```python
continuity_cnt = (
    scd.withColumn("next_start", F.lead("effective_start_date").over(by_emp))
    .filter(
        F.col("next_start").isNotNull()
        & (F.date_add(F.col("effective_end_date"), 1) != F.col("next_start"))
    )
    .count()
)
```

**What it does:** Ensures there are no missing days between versions. It mathematically adds 1 day to the `effective_end_date` and checks if it perfectly matches the next row's `effective_start_date`.
**Why it matters:** If row 1 ends on **Jan 4**, row 2 *must* start on **Jan 5**. If row 2 starts on Jan 10 instead, the employee is "missing" between Jan 5 and Jan 9. If an HR manager filters a report for Jan 7th, that employee's data will vanish!

---

### 5. Static & Fact Table Basic Checks

```python
static_null_cnt = static.filter(F.col("employee_id").isNull()).count()
# ...
daily_null_cnt = daily.filter(F.col("employee_id").isNull() | F.col("snapshot_date").isNull()).count()
```

**What it does:** Basic sanity checks ensuring our static dimension (`gold_dim_employee_static`) and daily fact table (`gold_fact_employee_daily_snapshot`) aren't missing their primary joining keys.

---

### Summary
When this testing script prints `"Validation passed"`, it acts as a guarantee that the PySpark transformation was mathematically perfect. The timelines are seamless, continuous, and non-overlapping, meaning the data is completely safe to load into Power BI.