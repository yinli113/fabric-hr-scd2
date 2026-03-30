#!/usr/bin/env python3
from __future__ import annotations

"""Notebook 02 - Silver stage."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

DEFAULT_BRONZE_TABLE = "bronze_employee_hr_raw_extract"

REQUIRED_RAW_COLUMNS = [
    "employee_id",
    "snapshot_date",
    "status",
    "manager_id",
    "team",
    "salary_band",
    "fte_flag",
    "work_mode",
    "org_unit",
    "role_family",
    "location",
    "hire_date",
    "date_of_birth",
    "gender",
    "full_name",
    "personal_email",
    "work_email",
    "phone_number",
    "home_address_line1",
    "home_address_city",
    "home_address_state",
    "home_address_postal_code",
    "tax_id",
]

DEFAULT_SILVER_SNAPSHOT_TABLE = "silver_employee_daily_snapshot"
DEFAULT_SILVER_BASE_TABLE = "silver_employees_base"
DEFAULT_SILVER_EVENTS_TABLE = "silver_employee_change_events"


def validate_required_columns(df: DataFrame) -> None:
    missing = [c for c in REQUIRED_RAW_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required raw columns: {missing}")


def build_snapshot_silver(df_raw: DataFrame) -> DataFrame:
    return (
        df_raw.withColumn("snapshot_date", F.to_date("snapshot_date"))
        .withColumn("hire_date", F.to_date("hire_date"))
        .withColumn("date_of_birth", F.to_date("date_of_birth"))
        .withColumn("fte_flag", F.col("fte_flag").cast("int"))
        .withColumn("ingest_ts", F.current_timestamp())
        .filter(F.col("employee_id").isNotNull())
        .filter(F.col("snapshot_date").isNotNull())
        .dropDuplicates(["employee_id", "snapshot_date"])
    )


def build_static_base_silver(df_snapshot: DataFrame) -> DataFrame:
    first_snapshot = (
        df_snapshot.groupBy("employee_id")
        .agg(F.min("snapshot_date").alias("first_snapshot_date"))
        .alias("first_snapshot")
    )
    base = df_snapshot.alias("s").join(
        first_snapshot,
        (F.col("s.employee_id") == F.col("first_snapshot.employee_id"))
        & (F.col("s.snapshot_date") == F.col("first_snapshot.first_snapshot_date")),
        "inner",
    )
    return base.select(
        F.col("s.employee_id").alias("employee_id"),
        F.col("s.hire_date").alias("hire_date"),
        F.col("s.date_of_birth").alias("date_of_birth"),
        F.col("s.gender").alias("gender"),
        F.col("s.full_name").alias("full_name"),
        F.col("s.personal_email").alias("personal_email"),
        F.col("s.work_email").alias("work_email"),
        F.col("s.phone_number").alias("phone_number"),
        F.col("s.home_address_line1").alias("home_address_line1"),
        F.col("s.home_address_city").alias("home_address_city"),
        F.col("s.home_address_state").alias("home_address_state"),
        F.col("s.home_address_postal_code").alias("home_address_postal_code"),
        F.col("s.tax_id").alias("tax_id"),
        F.col("s.org_unit").alias("org_unit"),
        F.col("s.role_family").alias("role_family"),
        F.col("s.location").alias("location"),
    ).dropDuplicates(["employee_id"])


def build_change_events_silver(df_snapshot: DataFrame) -> DataFrame:
    with_hash = (
        df_snapshot.select(
            "employee_id",
            "snapshot_date",
            "status",
            "manager_id",
            "team",
            "salary_band",
            "fte_flag",
            "work_mode",
            "org_unit",
            "role_family",
            "location",
            "phone_number",
            "home_address_line1",
            "home_address_city",
            "home_address_state",
            "home_address_postal_code",
        )
        .withColumn(
            "tracked_hash",
            F.sha2(
                F.concat_ws(
                    "|",
                    "status",
                    "manager_id",
                    "team",
                    "salary_band",
                    F.col("fte_flag").cast("string"),
                    "work_mode",
                    "org_unit",
                    "role_family",
                    "location",
                    "phone_number",
                    "home_address_line1",
                    "home_address_city",
                    "home_address_state",
                    "home_address_postal_code",
                ),
                256,
            ),
        )
    )

    by_emp = Window.partitionBy("employee_id").orderBy("snapshot_date")
    with_prev = (
        with_hash.withColumn("prev_hash", F.lag("tracked_hash").over(by_emp))
        .withColumn("prev_status", F.lag("status").over(by_emp))
        .withColumn("prev_manager_id", F.lag("manager_id").over(by_emp))
        .withColumn("prev_team", F.lag("team").over(by_emp))
        .withColumn("prev_salary_band", F.lag("salary_band").over(by_emp))
        .withColumn("prev_fte_flag", F.lag("fte_flag").over(by_emp))
        .withColumn("prev_work_mode", F.lag("work_mode").over(by_emp))
        .withColumn("prev_org_unit", F.lag("org_unit").over(by_emp))
        .withColumn("prev_role_family", F.lag("role_family").over(by_emp))
        .withColumn("prev_location", F.lag("location").over(by_emp))
        .withColumn("prev_phone_number", F.lag("phone_number").over(by_emp))
        .withColumn("prev_home_address_line1", F.lag("home_address_line1").over(by_emp))
        .withColumn("prev_home_address_city", F.lag("home_address_city").over(by_emp))
        .withColumn("prev_home_address_state", F.lag("home_address_state").over(by_emp))
        .withColumn("prev_home_address_postal_code", F.lag("home_address_postal_code").over(by_emp))
    )

    changed = with_prev.filter(
        (F.col("prev_hash").isNull()) | (F.col("tracked_hash") != F.col("prev_hash"))
    )
    return changed.select(
        "employee_id",
        F.col("snapshot_date").alias("event_date"),
        "prev_status",
        "status",
        "prev_manager_id",
        "manager_id",
        "prev_team",
        "team",
        "prev_salary_band",
        "salary_band",
        "prev_fte_flag",
        "fte_flag",
        "prev_work_mode",
        "work_mode",
        "prev_org_unit",
        "org_unit",
        "prev_role_family",
        "role_family",
        "prev_location",
        "location",
        "prev_phone_number",
        "phone_number",
        "prev_home_address_line1",
        "home_address_line1",
        "prev_home_address_city",
        "home_address_city",
        "prev_home_address_state",
        "home_address_state",
        "prev_home_address_postal_code",
        "home_address_postal_code",
        "tracked_hash",
    )


def silver_from_bronze(
    spark,
    bronze_table: str = DEFAULT_BRONZE_TABLE,
    silver_snapshot_table: str = DEFAULT_SILVER_SNAPSHOT_TABLE,
    silver_base_table: str = DEFAULT_SILVER_BASE_TABLE,
    silver_events_table: str = DEFAULT_SILVER_EVENTS_TABLE,
) -> None:
    df_raw = spark.table(bronze_table)
    validate_required_columns(df_raw)

    df_snapshot = build_snapshot_silver(df_raw).cache()
    df_base = build_static_base_silver(df_snapshot)
    df_events = build_change_events_silver(df_snapshot)

    df_snapshot.write.mode("overwrite").format("delta").saveAsTable(silver_snapshot_table)
    df_base.write.mode("overwrite").format("delta").saveAsTable(silver_base_table)
    df_events.write.mode("overwrite").format("delta").saveAsTable(silver_events_table)

    print(
        "Silver build complete:\n"
        f"- source: {bronze_table}\n"
        f"- {silver_snapshot_table}: {df_snapshot.count()} rows\n"
        f"- {silver_base_table}: {df_base.count()} rows\n"
        f"- {silver_events_table}: {df_events.count()} rows"
    )


def run_nb_02(
    spark,
    bronze_table: str = DEFAULT_BRONZE_TABLE,
    silver_snapshot_table: str = DEFAULT_SILVER_SNAPSHOT_TABLE,
    silver_base_table: str = DEFAULT_SILVER_BASE_TABLE,
    silver_events_table: str = DEFAULT_SILVER_EVENTS_TABLE,
) -> None:
    """
    One-call entrypoint for Notebook 02.
    """
    silver_from_bronze(
        spark,
        bronze_table=bronze_table,
        silver_snapshot_table=silver_snapshot_table,
        silver_base_table=silver_base_table,
        silver_events_table=silver_events_table,
    )
    spark.sql("SHOW TABLES").show(truncate=False)

__all__ = [
    "DEFAULT_SILVER_SNAPSHOT_TABLE",
    "DEFAULT_SILVER_BASE_TABLE",
    "DEFAULT_SILVER_EVENTS_TABLE",
    "build_snapshot_silver",
    "build_static_base_silver",
    "build_change_events_silver",
    "silver_from_bronze",
    "run_nb_02",
]

