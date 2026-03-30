#!/usr/bin/env python3
from __future__ import annotations

"""Notebook 04 - Gold validation tests."""

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def validate_gold_tables(
    spark,
    gold_scd2_table: str = "gold_dim_employee_scd2",
    gold_static_table: str = "gold_dim_employee_static",
    gold_daily_fact_table: str = "gold_fact_employee_daily_snapshot",
) -> None:
    scd = spark.table(gold_scd2_table).cache()
    static = spark.table(gold_static_table)
    daily = spark.table(gold_daily_fact_table)

    null_key_cnt = scd.filter(
        F.col("employee_id").isNull() | F.col("effective_start_date").isNull() | F.col("effective_end_date").isNull()
    ).count()
    if null_key_cnt > 0:
        raise AssertionError(f"{gold_scd2_table} contains {null_key_cnt} null key/date rows")

    by_emp = Window.partitionBy("employee_id").orderBy("effective_start_date")
    overlap_cnt = (
        scd.withColumn("next_start", F.lead("effective_start_date").over(by_emp))
        .filter(F.col("next_start").isNotNull() & (F.col("effective_end_date") >= F.col("next_start")))
        .count()
    )
    if overlap_cnt > 0:
        raise AssertionError(f"{gold_scd2_table} overlap check failed: {overlap_cnt} overlapping ranges")

    current_cnt = (
        scd.filter(F.col("is_current") == True)  # noqa: E712
        .groupBy("employee_id")
        .count()
        .filter(F.col("count") != 1)
        .count()
    )
    if current_cnt > 0:
        raise AssertionError(f"{gold_scd2_table} current row check failed for {current_cnt} employees")

    continuity_cnt = (
        scd.withColumn("next_start", F.lead("effective_start_date").over(by_emp))
        .filter(
            F.col("next_start").isNotNull()
            & (F.date_add(F.col("effective_end_date"), 1) != F.col("next_start"))
        )
        .count()
    )
    if continuity_cnt > 0:
        raise AssertionError(f"{gold_scd2_table} continuity check failed: {continuity_cnt} gaps found")

    static_null_cnt = static.filter(F.col("employee_id").isNull()).count()
    if static_null_cnt > 0:
        raise AssertionError(f"{gold_static_table} has {static_null_cnt} null employee_id rows")

    daily_null_cnt = daily.filter(F.col("employee_id").isNull() | F.col("snapshot_date").isNull()).count()
    if daily_null_cnt > 0:
        raise AssertionError(f"{gold_daily_fact_table} has {daily_null_cnt} null employee/date rows")

    print(
        "Validation passed:\n"
        f"- {gold_scd2_table}: no overlaps, continuity OK, one current row per employee\n"
        f"- {gold_static_table}: keys present\n"
        f"- {gold_daily_fact_table}: keys present"
    )


def run_nb_04(
    spark,
    gold_scd2_table: str = "gold_dim_employee_scd2",
    gold_static_table: str = "gold_dim_employee_static",
    gold_daily_fact_table: str = "gold_fact_employee_daily_snapshot",
) -> None:
    """
    One-call entrypoint for Notebook 04.
    """
    validate_gold_tables(
        spark,
        gold_scd2_table=gold_scd2_table,
        gold_static_table=gold_static_table,
        gold_daily_fact_table=gold_daily_fact_table,
    )
    spark.sql("SHOW TABLES").show(truncate=False)

__all__ = ["validate_gold_tables", "run_nb_04"]

