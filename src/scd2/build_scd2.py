#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List


ROOT = Path(__file__).resolve().parents[2]
RAW_SNAPSHOT_PATH = ROOT / "data" / "raw" / "employee_daily_snapshot.csv"
OUTPUT_PATH = ROOT / "data" / "processed" / "dim_employee_scd2.csv"


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


def read_snapshot_rows() -> List[Dict]:
    with RAW_SNAPSHOT_PATH.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def hash_diff(row: Dict) -> str:
    key = "|".join(str(row[k]) for k in TRACKED_FIELDS)
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def write_rows(path: Path, rows: List[Dict], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    rows = read_snapshot_rows()
    if not rows:
        raise RuntimeError("No snapshot rows found. Run data generator first.")

    rows.sort(key=lambda r: (r["employee_id"], r["snapshot_date"]))
    scd_rows: List[Dict] = []

    current_by_emp: Dict[str, Dict] = {}
    sk_counter = 1

    for r in rows:
        emp_id = r["employee_id"]
        snap_date = date.fromisoformat(r["snapshot_date"])
        current_hash = hash_diff(r)

        active = current_by_emp.get(emp_id)
        if active is None:
            current_by_emp[emp_id] = {
                "employee_sk": str(sk_counter),
                "employee_id": emp_id,
                "effective_start_date": r["snapshot_date"],
                "effective_end_date": "9999-12-31",
                "is_current": "true",
                "hash_diff": current_hash,
                **{k: r[k] for k in TRACKED_FIELDS},
            }
            sk_counter += 1
            continue

        if active["hash_diff"] == current_hash:
            continue

        prev_end = (snap_date - timedelta(days=1)).isoformat()
        active["effective_end_date"] = prev_end
        active["is_current"] = "false"
        scd_rows.append(active)

        current_by_emp[emp_id] = {
            "employee_sk": str(sk_counter),
            "employee_id": emp_id,
            "effective_start_date": r["snapshot_date"],
            "effective_end_date": "9999-12-31",
            "is_current": "true",
            "hash_diff": current_hash,
            **{k: r[k] for k in TRACKED_FIELDS},
        }
        sk_counter += 1

    scd_rows.extend(current_by_emp.values())
    scd_rows.sort(key=lambda r: (r["employee_id"], r["effective_start_date"]))

    fieldnames = [
        "employee_sk",
        "employee_id",
        "effective_start_date",
        "effective_end_date",
        "is_current",
        "hash_diff",
        *TRACKED_FIELDS,
    ]
    write_rows(OUTPUT_PATH, scd_rows, fieldnames)
    print(f"Built {len(scd_rows)} SCD2 rows -> {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
