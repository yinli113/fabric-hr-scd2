#!/usr/bin/env python3
from __future__ import annotations

import csv
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List


ROOT = Path(__file__).resolve().parents[1]
SCD_PATH = ROOT / "data" / "processed" / "dim_employee_scd2.csv"


def load_rows() -> List[Dict]:
    with SCD_PATH.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def assert_no_overlap(rows: List[Dict]) -> None:
    grouped = defaultdict(list)
    for r in rows:
        grouped[r["employee_id"]].append(r)
    for emp, items in grouped.items():
        items.sort(key=lambda x: x["effective_start_date"])
        for i in range(len(items) - 1):
            cur_end = date.fromisoformat(items[i]["effective_end_date"])
            next_start = date.fromisoformat(items[i + 1]["effective_start_date"])
            assert cur_end < next_start, f"Overlap detected for {emp}"


def assert_single_current(rows: List[Dict]) -> None:
    grouped = defaultdict(int)
    for r in rows:
        if r["is_current"] == "true":
            grouped[r["employee_id"]] += 1
    for emp, cnt in grouped.items():
        assert cnt == 1, f"Expected one current row for {emp}, got {cnt}"


def assert_continuity(rows: List[Dict]) -> None:
    grouped = defaultdict(list)
    for r in rows:
        grouped[r["employee_id"]].append(r)
    for emp, items in grouped.items():
        items.sort(key=lambda x: x["effective_start_date"])
        for i in range(len(items) - 1):
            end_d = date.fromisoformat(items[i]["effective_end_date"])
            next_start = date.fromisoformat(items[i + 1]["effective_start_date"])
            assert end_d + timedelta(days=1) == next_start, f"Gap for {emp}"


def main() -> None:
    rows = load_rows()
    assert rows, "No SCD2 rows found."
    assert_no_overlap(rows)
    assert_single_current(rows)
    assert_continuity(rows)
    print("All SCD2 integrity checks passed.")


if __name__ == "__main__":
    main()
