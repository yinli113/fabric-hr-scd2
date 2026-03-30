#!/usr/bin/env python3
from __future__ import annotations

"""Notebook 03 - Gold build stage."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

DEFAULT_SILVER_SNAPSHOT_TABLE = "silver_employee_daily_snapshot"
DEFAULT_SILVER_BASE_TABLE = "silver_employees_base"
DEFAULT_SILVER_EVENTS_TABLE = "silver_employee_change_events"

MASK_SALT = "employee-scd2-lab-static-salt"

TRACKED_FIELDS = [
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
]

DEFAULT_GOLD_STATIC_TABLE = "gold_dim_employee_static"
DEFAULT_GOLD_SCD2_TABLE = "gold_dim_employee_scd2"
DEFAULT_GOLD_DAILY_FACT = "gold_fact_employee_daily_snapshot"
DEFAULT_GOLD_REPORTING_FACT = "gold_fact_reporting_line_daily"
DEFAULT_GOLD_CHANGE_FACT = "gold_fact_employee_change_event"


def _stable_token(col_name: str):
    return F.sha2(F.concat_ws("|", F.lit(MASK_SALT), F.coalesce(F.col(col_name), F.lit(""))), 256)


def _mask_email(col_name: str):
    return F.when(
        F.col(col_name).isNull() | (F.trim(F.col(col_name)) == ""),
        F.lit("REDACTED_EMAIL"),
    ).otherwise(
        F.concat(
            F.substring(F.regexp_replace(F.split(F.col(col_name), "@").getItem(0), "[^a-zA-Z0-9]", ""), 1, 1),
            F.lit("***"),
            F.reverse(
                F.substring(
                    F.reverse(F.regexp_replace(F.split(F.col(col_name), "@").getItem(0), "[^a-zA-Z0-9]", "")),
                    1,
                    1,
                )
            ),
            F.lit("@"),
            F.split(F.col(col_name), "@").getItem(1),
        )
    )


def build_dim_employee_static(silver_base: DataFrame) -> DataFrame:
    return (
        silver_base.withColumn("employee_static_sk", F.row_number().over(Window.orderBy("employee_id")))
        .withColumn("birth_year_masked", F.year("date_of_birth").cast("string"))
        .withColumn("full_name_masked", F.col("full_name")) # Name unmasked
        .withColumn("work_email_masked", _mask_email("work_email"))
        .withColumn("personal_email_masked", _mask_email("personal_email"))
        .withColumn(
            "tax_id_last4",
            F.when(F.col("tax_id").isNull(), F.lit("")).otherwise(
                F.expr("substring(tax_id, greatest(length(tax_id) - 3, 1), 4)")
            ),
        )
        .withColumn("work_email_token", _stable_token("work_email"))
        .withColumn("tax_id_token", _stable_token("tax_id"))
        .select(
            "employee_static_sk",
            "employee_id",
            "birth_year_masked",
            "gender",
            "full_name_masked",
            "hire_date",
            "work_email_masked",
            "personal_email_masked",
            "tax_id_last4",
            "work_email_token",
            "tax_id_token",
        )
        .dropDuplicates(["employee_id"])
    )


def build_dim_employee_scd2(silver_snapshot: DataFrame) -> DataFrame:
    # Need to map over tracked fields and ensure they are columns, not strings, inside concat_ws
    tracked_cols = [F.col(c).cast("string") for c in TRACKED_FIELDS]
    tracked_hash = F.sha2(F.concat_ws("|", *tracked_cols), 256)
    base = silver_snapshot.select("employee_id", "snapshot_date", *TRACKED_FIELDS).withColumn(
        "hash_diff", tracked_hash
    )
    by_emp = Window.partitionBy("employee_id").orderBy("snapshot_date")
    with_versions = (
        base.withColumn("prev_hash", F.lag("hash_diff").over(by_emp))
        .withColumn(
            "is_new_version",
            F.when(F.col("prev_hash").isNull() | (F.col("hash_diff") != F.col("prev_hash")), 1).otherwise(0),
        )
        .withColumn("version_num", F.sum("is_new_version").over(by_emp))
    )
    agg_cols = [
        F.min("snapshot_date").alias("effective_start_date"),
        F.max("snapshot_date").alias("last_seen_snapshot_date")
    ]
    for c in TRACKED_FIELDS:
        agg_cols.append(F.first(c, ignorenulls=True).alias(c))
    agg_cols.append(F.first("hash_diff", ignorenulls=True).alias("hash_diff"))

    versions = with_versions.groupBy("employee_id", "version_num").agg(*agg_cols)
    by_ver = Window.partitionBy("employee_id").orderBy("effective_start_date")
    return (
        versions.withColumn("next_start_date", F.lead("effective_start_date").over(by_ver))
        .withColumn(
            "effective_end_date",
            F.when(F.col("next_start_date").isNull(), F.to_date(F.lit("9999-12-31"))).otherwise(
                F.date_sub(F.col("next_start_date"), 1)
            ),
        )
        .withColumn("is_current", F.when(F.col("next_start_date").isNull(), F.lit(True)).otherwise(F.lit(False)))
        .withColumn("employee_sk", F.row_number().over(Window.orderBy("employee_id", "effective_start_date")))
        .select(
            "employee_sk",
            "employee_id",
            "effective_start_date",
            "effective_end_date",
            "is_current",
            "hash_diff",
            *TRACKED_FIELDS,
        )
    )


def build_gold_from_silver(
    spark,
    silver_snapshot_table: str = DEFAULT_SILVER_SNAPSHOT_TABLE,
    silver_base_table: str = DEFAULT_SILVER_BASE_TABLE,
    silver_events_table: str = DEFAULT_SILVER_EVENTS_TABLE,
    gold_static_table: str = DEFAULT_GOLD_STATIC_TABLE,
    gold_scd2_table: str = DEFAULT_GOLD_SCD2_TABLE,
    gold_daily_fact_table: str = DEFAULT_GOLD_DAILY_FACT,
    gold_reporting_fact_table: str = DEFAULT_GOLD_REPORTING_FACT,
    gold_change_fact_table: str = DEFAULT_GOLD_CHANGE_FACT,
) -> None:
    silver_snapshot = spark.table(silver_snapshot_table)
    silver_base = spark.table(silver_base_table)
    silver_events = spark.table(silver_events_table)

    dim_static = build_dim_employee_static(silver_base)
    dim_scd2 = build_dim_employee_scd2(silver_snapshot)
    fact_daily = silver_snapshot.select(
        F.date_format("snapshot_date", "yyyyMMdd").cast("int").alias("date_key"),
        "snapshot_date",
        "employee_id",
        "status",
        "org_unit",
        "team",
        "work_mode",
        "fte_flag",
        "salary_band",
        F.when(F.col("status") == "active", F.lit(1)).otherwise(F.lit(0)).alias("active_headcount_flag"),
    )
    fact_reporting = silver_snapshot.select(
        F.date_format("snapshot_date", "yyyyMMdd").cast("int").alias("date_key"),
        "snapshot_date",
        F.col("employee_id").alias("employee_id"),
        F.col("manager_id").alias("manager_employee_id"),
        "org_unit",
        "team",
    )
    fact_change = silver_events.select(
        F.date_format("event_date", "yyyyMMdd").cast("int").alias("date_key"),
        "event_date",
        "employee_id",
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
    )

    dim_static.write.mode("overwrite").format("delta").saveAsTable(gold_static_table)
    dim_scd2.write.mode("overwrite").format("delta").saveAsTable(gold_scd2_table)
    fact_daily.write.mode("overwrite").format("delta").saveAsTable(gold_daily_fact_table)
    fact_reporting.write.mode("overwrite").format("delta").saveAsTable(gold_reporting_fact_table)
    fact_change.write.mode("overwrite").format("delta").saveAsTable(gold_change_fact_table)

    print(
        "Gold build complete:\n"
        f"- {gold_static_table}: {dim_static.count()} rows\n"
        f"- {gold_scd2_table}: {dim_scd2.count()} rows\n"
        f"- {gold_daily_fact_table}: {fact_daily.count()} rows\n"
        f"- {gold_reporting_fact_table}: {fact_reporting.count()} rows\n"
        f"- {gold_change_fact_table}: {fact_change.count()} rows"
    )


def run_nb_03(
    spark,
    silver_snapshot_table: str = DEFAULT_SILVER_SNAPSHOT_TABLE,
    silver_base_table: str = DEFAULT_SILVER_BASE_TABLE,
    silver_events_table: str = DEFAULT_SILVER_EVENTS_TABLE,
    gold_static_table: str = DEFAULT_GOLD_STATIC_TABLE,
    gold_scd2_table: str = DEFAULT_GOLD_SCD2_TABLE,
    gold_daily_fact_table: str = DEFAULT_GOLD_DAILY_FACT,
    gold_reporting_fact_table: str = DEFAULT_GOLD_REPORTING_FACT,
    gold_change_fact_table: str = DEFAULT_GOLD_CHANGE_FACT,
) -> None:
    """
    One-call entrypoint for Notebook 03.
    """
    build_gold_from_silver(
        spark,
        silver_snapshot_table=silver_snapshot_table,
        silver_base_table=silver_base_table,
        silver_events_table=silver_events_table,
        gold_static_table=gold_static_table,
        gold_scd2_table=gold_scd2_table,
        gold_daily_fact_table=gold_daily_fact_table,
        gold_reporting_fact_table=gold_reporting_fact_table,
        gold_change_fact_table=gold_change_fact_table,
    )
    spark.sql("SHOW TABLES").show(truncate=False)

__all__ = [
    "TRACKED_FIELDS",
    "DEFAULT_GOLD_STATIC_TABLE",
    "DEFAULT_GOLD_SCD2_TABLE",
    "DEFAULT_GOLD_DAILY_FACT",
    "DEFAULT_GOLD_REPORTING_FACT",
    "DEFAULT_GOLD_CHANGE_FACT",
    "build_dim_employee_static",
    "build_dim_employee_scd2",
    "build_gold_from_silver",
    "run_nb_03",
]

