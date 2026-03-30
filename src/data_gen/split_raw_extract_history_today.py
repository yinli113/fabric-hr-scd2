#!/usr/bin/env python3
"""
Split employee_hr_raw_extract.csv into:
- history: all rows where snapshot_date < max(snapshot_date) in the file
- today:   all rows where snapshot_date == max(snapshot_date)  (incremental / latest day)

Use after generate_employee_data.py. Writes to data/raw/ and data/adls_gen2/raw/.
"""
from __future__ import annotations

import csv
from pathlib import Path
from typing import List, Dict

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "data" / "raw" / "employee_hr_raw_extract.csv"
ADLS = ROOT / "data" / "adls_gen2" / "raw"
OUT_HISTORY = "employee_hr_raw_extract_history.csv"
OUT_TODAY = "employee_hr_raw_extract_today.csv"


def main() -> None:
    if not RAW.exists():
        raise SystemExit(f"Missing {RAW}. Run: python3 src/data_gen/generate_employee_data.py")

    with RAW.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames: List[str] = list(reader.fieldnames or [])
        rows: List[Dict[str, str]] = list(reader)

    if not rows:
        raise SystemExit("No rows in raw extract.")

    dates = sorted({r["snapshot_date"] for r in rows})
    max_date = dates[-1]
    history_rows = [r for r in rows if r["snapshot_date"] < max_date]
    today_rows = [r for r in rows if r["snapshot_date"] == max_date]

    for out_name, subset in [(OUT_HISTORY, history_rows), (OUT_TODAY, today_rows)]:
        for dest in (ROOT / "data" / "raw", ADLS):
            dest.mkdir(parents=True, exist_ok=True)
            path = dest / out_name
            with path.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames)
                w.writeheader()
                w.writerows(subset)

    print(f"max(snapshot_date) = {max_date} (treated as 'today' slice for incremental ingest)")
    print(f"  history rows: {len(history_rows)} -> {OUT_HISTORY}")
    print(f"  today rows:   {len(today_rows)} -> {OUT_TODAY}")


if __name__ == "__main__":
    main()
