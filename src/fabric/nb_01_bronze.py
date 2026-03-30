#!/usr/bin/env python3
from __future__ import annotations

"""Notebook 01 - Bronze stage."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

DEFAULT_BRONZE_SOURCE_PATH = "Files/employee_hr_raw_extract_history.csv"
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


def validate_required_columns(df: DataFrame) -> None:
    missing = [c for c in REQUIRED_RAW_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required raw columns: {missing}")


def _normalize_header_bom(df: DataFrame) -> DataFrame:
    # Some CSV exports include BOM in the first header.
    if "\ufeffemployee_id" in df.columns and "employee_id" not in df.columns:
        return df.withColumnRenamed("\ufeffemployee_id", "employee_id")
    return df


def read_raw_data(
    spark,
    source_path: str = DEFAULT_BRONZE_SOURCE_PATH,
    source_format: str = "auto",
) -> DataFrame:
    """
    Read raw source as Delta or CSV.

    source_format:
      - "auto": try Delta first, then CSV
      - "delta": only Delta
      - "csv": only CSV
    """
    if source_format not in {"auto", "delta", "csv"}:
        raise ValueError("source_format must be one of: auto, delta, csv")

    errors = []

    if source_format in {"auto", "delta"}:
        try:
            df_delta = spark.read.format("delta").load(source_path)
            df_delta = _normalize_header_bom(df_delta)
            validate_required_columns(df_delta)
            return df_delta
        except Exception as e:  # noqa: BLE001
            errors.append(f"delta read failed: {e}")
            if source_format == "delta":
                raise

    if source_format in {"auto", "csv"}:
        # Avoid building invalid paths like '*.csv/*.csv'.
        candidate_paths = [source_path]
        source_lower = source_path.lower()
        if "*" not in source_path and not source_lower.endswith(".csv"):
            candidate_paths.append(f"{source_path}/*.csv")
        for p in candidate_paths:
            try:
                df_csv = (
                    spark.read.option("header", True)
                    .option("sep", ",")
                    .option("quote", '"')
                    .option("escape", '"')
                    .option("multiLine", False)
                    .csv(p)
                )
                df_csv = _normalize_header_bom(df_csv)
                validate_required_columns(df_csv)
                return df_csv
            except Exception as e:  # noqa: BLE001
                errors.append(f"csv read failed for {p}: {e}")
                continue

    raise ValueError(
        "Unable to read required HR columns from source path. "
        "If your shortcut has _delta_log under Files/rawdev, use source_path='Files/rawdev' "
        "and source_format='auto' or 'delta'. "
        f"Details: {' | '.join(errors)}"
    )


def read_raw_csv(spark, source_path: str = DEFAULT_BRONZE_SOURCE_PATH) -> DataFrame:
    # Backward-compatible helper for explicit CSV mode.
    return read_raw_data(spark, source_path=source_path, source_format="csv")


def bronze_register_table(
    spark,
    source_path: str = DEFAULT_BRONZE_SOURCE_PATH,
    bronze_table: str = DEFAULT_BRONZE_TABLE,
    source_format: str = "delta",
) -> None:
    """
    Register a Bronze table over an existing path (no heavy data rewrite).

    Use this when the source shortcut already points to Delta data (recommended for OneLake shortcut paths).
    """
    df_raw = read_raw_data(spark, source_path=source_path, source_format=source_format)
    validate_required_columns(df_raw)
    # Prefer view registration for OneLake shortcut paths to avoid LOCATION root parsing issues.
    spark.sql(f"DROP VIEW IF EXISTS {bronze_table}")
    spark.sql(
        f"""
        CREATE VIEW {bronze_table} AS
        SELECT * FROM delta.`{source_path}`
        """
    )
    print(
        "Bronze view registered:\n"
        f"- source: {source_path}\n"
        f"- object: {bronze_table}\n"
        f"- rows visible: {df_raw.count()}"
    )


def bronze_to_table(
    spark,
    source_path: str = DEFAULT_BRONZE_SOURCE_PATH,
    bronze_table: str = DEFAULT_BRONZE_TABLE,
    source_format: str = "auto",
    write_mode: str = "overwrite",
) -> None:
    df_raw = read_raw_data(spark, source_path=source_path, source_format=source_format)
    df_bronze = df_raw.withColumn("ingest_ts", F.current_timestamp()).withColumn(
        "source_file", F.lit(source_path)
    )
    df_bronze.write.mode(write_mode).format("delta").saveAsTable(bronze_table)
    print(f"Bronze load complete:\n- source: {source_path}\n- mode: {write_mode}\n- table: {bronze_table}\n- rows: {df_bronze.count()}")


def run_nb_01(
    spark,
    source_path: str = DEFAULT_BRONZE_SOURCE_PATH,
    bronze_table: str = DEFAULT_BRONZE_TABLE,
    mode: str = "copy",
    source_format: str = "csv",
    write_mode: str = "overwrite",
) -> None:
    """
    One-call entrypoint for Notebook 01.

    mode:
      - "register": create table metadata over existing Delta path (best for shortcut + _delta_log)
      - "copy": read source and write a managed Bronze table
    """
    if mode not in {"register", "copy"}:
        raise ValueError("mode must be one of: register, copy")
    if mode == "register":
        # register mode is typically used with a Delta-backed shortcut path
        bronze_register_table(
            spark,
            source_path=source_path,
            bronze_table=bronze_table,
            source_format="delta" if source_format == "auto" else source_format,
        )
    else:
        bronze_to_table(
            spark,
            source_path=source_path,
            bronze_table=bronze_table,
            source_format=source_format,
            write_mode=write_mode,
        )
    spark.sql("SHOW TABLES").show(truncate=False)

__all__ = [
    "DEFAULT_BRONZE_SOURCE_PATH",
    "DEFAULT_BRONZE_TABLE",
    "REQUIRED_RAW_COLUMNS",
    "validate_required_columns",
    "read_raw_data",
    "read_raw_csv",
    "bronze_register_table",
    "bronze_to_table",
    "run_nb_01",
]

