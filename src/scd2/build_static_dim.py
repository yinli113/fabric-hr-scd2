#!/usr/bin/env python3
from __future__ import annotations

import csv
import sys
from pathlib import Path
from typing import Dict, List

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.security.masking import (  # noqa: E402
    birth_year_deterministic,
    mask_email_deterministic,
    mask_full_name_deterministic,
    mask_phone_deterministic,
    mask_postal_deterministic,
    sha256_token,
)
BASE_PATH = ROOT / "data" / "raw" / "employees_base.csv"
OUTPUT_PATH = ROOT / "data" / "processed" / "dim_employee_static.csv"


def read_base_rows() -> List[Dict]:
    with BASE_PATH.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_rows(path: Path, rows: List[Dict], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    rows = read_base_rows()
    if not rows:
        raise RuntimeError("No base employees found. Run data generator first.")

    out_rows: List[Dict] = []
    for i, r in enumerate(rows, start=1):
        # The static dimension is the place where we intentionally avoid exposing raw PII.
        work_email_masked = mask_email_deterministic(r.get("work_email", ""))
        personal_email_masked = mask_email_deterministic(r.get("personal_email", ""))
        phone_masked = mask_phone_deterministic(r.get("phone_number", ""))
        postal_masked = mask_postal_deterministic(r.get("home_address_postal_code", ""))
        full_name_masked = mask_full_name_deterministic(r.get("full_name", ""))
        birth_year_masked = birth_year_deterministic(r.get("date_of_birth", ""))

        tax_id = r.get("tax_id", "")
        tax_id_last4 = tax_id[-4:] if len(tax_id) >= 4 else ""

        out_rows.append(
            {
                "employee_static_sk": str(i),
                "employee_id": r["employee_id"],
                "birth_year_masked": birth_year_masked,
                "gender": r["gender"],
                "full_name_masked": full_name_masked,
                "hire_date": r["hire_date"],
                "org_unit_at_hire": r.get("org_unit", ""),
                "role_family_at_hire": r.get("role_family", ""),
                "location_at_hire": r.get("location", ""),
                "work_email_masked": work_email_masked,
                "personal_email_masked": personal_email_masked,
                "phone_masked": phone_masked,
                "home_address_city": r.get("home_address_city", ""),
                "home_address_state": r.get("home_address_state", ""),
                "home_address_postal_masked": postal_masked,
                "tax_id_last4": tax_id_last4,
                "work_email_token": sha256_token(r.get("work_email", "")),
                "phone_token": sha256_token(r.get("phone_number", "")),
                "tax_id_token": sha256_token(tax_id),
            }
        )

    fieldnames = [
        "employee_static_sk",
        "employee_id",
        "birth_year_masked",
        "gender",
        "full_name_masked",
        "hire_date",
        "org_unit_at_hire",
        "role_family_at_hire",
        "location_at_hire",
        "work_email_masked",
        "personal_email_masked",
        "phone_masked",
        "home_address_city",
        "home_address_state",
        "home_address_postal_masked",
        "tax_id_last4",
        "work_email_token",
        "phone_token",
        "tax_id_token",
    ]
    write_rows(OUTPUT_PATH, out_rows, fieldnames)
    print(f"Built {len(out_rows)} static dimension rows -> {OUTPUT_PATH}")


if __name__ == "__main__":
    main()

