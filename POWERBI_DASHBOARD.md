# Enterprise HR Analytics Dashboard

## Name: Global Workforce & Org Dynamics

## Core KPI Ribbon (Top of Dashboard)
Create 5 standard Power BI "Card" visuals across the top:
1. **Active Headcount** (The DAX measure we wrote earlier)
2. **Total Terminations** (The DAX measure we wrote earlier)
3. **Employees On Leave**
4. **Internal Mobility (Promotions/Transfers)** 
5. **Current Average Salary**

*Here is the DAX for Employees On Leave:*
```dax
Employees On Leave = 
CALCULATE(
    COUNTROWS(gold_fact_employee_daily_snapshot),
    gold_fact_employee_daily_snapshot[status] = "on_leave",
    gold_fact_employee_daily_snapshot[snapshot_date] = MAX(gold_fact_employee_daily_snapshot[snapshot_date])
)
```

*Here is the DAX for Internal Mobility:*
```dax
Internal Moves = 
CALCULATE(
    COUNTROWS(gold_fact_employee_change_event),
    gold_fact_employee_change_event[prev_org_unit] <> gold_fact_employee_change_event[org_unit] 
    || gold_fact_employee_change_event[prev_role_family] <> gold_fact_employee_change_event[role_family]
)
```

## Recommended Charts (The Canvas)

### 1. Headcount Growth Over Time (Area Chart)
* **X-Axis:** `snapshot_date` (from `gold_fact_employee_daily_snapshot` - set to Year/Month hierarchy)
* **Y-Axis:** `Active Headcount` (Measure)
* **Legend (Optional):** `work_mode` (to see the shift to Remote vs Office over time!)
* **Why it's great:** Because you built a daily fact table, this chart will render perfectly smooth trend lines without complex DAX time-intelligence formulas.

### 2. Organizational Structure Breakdown (Matrix or Tree Map)
* **Rows:** `org_unit`, then expand down to `team` (from `gold_fact_employee_daily_snapshot`)
* **Values:** `Active Headcount`
* **Why it's great:** Users can drill down from the department level all the way to specific teams to see exactly where headcount is concentrated.

### 3. Employee Lifecycle "Change Events" (Stacked Column Chart)
* **X-Axis:** `event_date` (from `gold_fact_employee_change_event` - set to Year/Month)
* **Y-Axis:** Count of `employee_id`
* **Legend:** Create a calculated column for "Event Type" (or just use `status` vs `prev_status`) to show a stacked bar of Hires vs Terminations vs Promotions every month.

### 4. Diversity & Demographics (Donut/Pie Chart)
* **Legend:** `gender` (from `gold_dim_employee_static`)
* **Values:** `Active Headcount`
* **Why it's great:** Because `gender` comes from the Static Dimension, filtering this chart will instantly filter the Headcount over Time chart to show the growth of specific demographics!

### 5. Dynamic Employee Profile (Table Visual)
Create a dynamic detail card that updates when you click an employee's name anywhere else on the dashboard.
1. Create a **Table** visual.
2. Create this DAX Measure in `gold_dim_employee_static`:
```dax
Employee Profile Details = 
VAR SelectedName = SELECTEDVALUE(gold_dim_employee_static[full_name_masked], "Select one employee")
VAR HasOnePerson = HASONEVALUE(gold_dim_employee_static[employee_id])

VAR Email = MAX(gold_dim_employee_static[work_email_masked])
VAR Phone = CALCULATE(MAX(gold_dim_employee_scd2[phone_number]), gold_dim_employee_scd2[is_current] = TRUE())
VAR WorkMode = CALCULATE(MAX(gold_dim_employee_scd2[work_mode]), gold_dim_employee_scd2[is_current] = TRUE())
VAR Address1 = CALCULATE(MAX(gold_dim_employee_scd2[home_address_line1]), gold_dim_employee_scd2[is_current] = TRUE())
VAR City = CALCULATE(MAX(gold_dim_employee_scd2[home_address_city]), gold_dim_employee_scd2[is_current] = TRUE())

RETURN
IF(
    HasOnePerson,
    "📧 Email: " & Email & UNICHAR(10) & 
    "📞 Phone: " & Phone & UNICHAR(10) &
    "🏢 Work Mode: " & WorkMode & UNICHAR(10) &
    "🏠 Address: " & Address1 & ", " & City,
    "Please select exactly one person."
)
```
3. Drag the measure into the Table visual. Now, clicking any name in the Decomposition Tree instantly displays their current contact info!

### 6. Dynamic Employee History Timeline (Table Visual)
Create a text timeline of the employee's career progression.
1. Create a **Table** visual.
2. Create this DAX Measure in `gold_dim_employee_static`:
```dax
Employee History Timeline = 
VAR SelectedName = SELECTEDVALUE(gold_dim_employee_static[full_name_masked], "")
VAR HasOnePerson = HASONEVALUE(gold_dim_employee_static[employee_id])

VAR HistoryText = 
    CONCATENATEX(
        gold_dim_employee_scd2,
        "• " & FORMAT(gold_dim_employee_scd2[effective_start_date], "yyyy-MM-dd") & " to " & 
        IF(gold_dim_employee_scd2[is_current], "Present", FORMAT(gold_dim_employee_scd2[effective_end_date], "yyyy-MM-dd")) & 
        UNICHAR(10) & "  └ " & gold_dim_employee_scd2[role_family] & " (" & gold_dim_employee_scd2[org_unit] & ")",
        UNICHAR(10) & UNICHAR(10), 
        gold_dim_employee_scd2[effective_start_date], 
        ASC
    )

RETURN
IF(
    HasOnePerson,
    "Career History for " & SelectedName & ":" & UNICHAR(10) & UNICHAR(10) & HistoryText,
    "Please select exactly one person to view history."
)
```
Make sure you have these slicers on the left side:
1. **As-Of Date Slider:** `snapshot_date` (Between slicer)
2. **Search Employee:** `full_name_masked` (from `gold_dim_employee_static` with the Search bar enabled)
3. **Department Filter:** `org_unit` (from the fact table)