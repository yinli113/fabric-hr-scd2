#!/usr/bin/env python3
from __future__ import annotations

"""Single entrypoint for notebook-ordered Fabric execution."""

from src.fabric.nb_01_bronze import DEFAULT_BRONZE_SOURCE_PATH, run_nb_01
from src.fabric.nb_02_silver import run_nb_02
from src.fabric.nb_03_gold import run_nb_03
from src.fabric.nb_04_tests import run_nb_04


def run_end_to_end(
    spark,
    bronze_csv_path: str = DEFAULT_BRONZE_SOURCE_PATH,
    bronze_table: str = "bronze_employee_hr_raw_extract",
    bronze_mode: str = "register",
) -> None:
    run_nb_01(
        spark,
        source_path=bronze_csv_path,
        bronze_table=bronze_table,
        mode=bronze_mode,
    )
    run_nb_02(spark, bronze_table=bronze_table)
    run_nb_03(spark)
    run_nb_04(spark)
    print("End-to-end pipeline finished successfully.")

