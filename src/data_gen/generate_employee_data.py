#!/usr/bin/env python3
from __future__ import annotations

import csv
import random
from dataclasses import dataclass, asdict
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List


ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "data" / "raw"
ADLS_DIR = ROOT / "data" / "adls_gen2"

ORG_UNITS = ["Finance", "Sales", "Engineering", "HR", "Operations"]
TEAMS = {
    "Finance": ["FP&A", "Accounting", "Treasury"],
    "Sales": ["SMB", "Enterprise", "Partnerships"],
    "Engineering": ["Platform", "Data", "Product"],
    "HR": ["Talent", "PeopleOps", "L&D"],
    "Operations": ["Support", "Procurement", "Facilities"],
}
WORK_MODES = ["remote", "hybrid", "onsite"]
SALARY_BANDS = ["B1", "B2", "B3", "B4", "B5"]
STATUSES = ["active", "on_leave", "terminated"]

FIRST_NAMES_F = ["Ava", "Mia", "Sophia", "Emma", "Olivia", "Grace", "Chloe", "Lily"]
FIRST_NAMES_M = ["Liam", "Noah", "Ethan", "James", "Oliver", "Henry", "Elijah", "Mateo"]
FIRST_NAMES_X = ["Alex", "Jordan", "Taylor", "Riley", "Casey", "Quinn", "Morgan"]
LAST_NAMES = ["Nguyen", "Patel", "Kim", "Smith", "Garcia", "Brown", "Singh", "Wright"]
GENDERS = ["F", "M", "X"]
ROLE_FAMILY_SUFFIXES = ["Specialist", "Senior_Specialist", "Manager", "Senior_Manager"]

STREET_NAMES = [
    "Main",
    "Oak",
    "Cedar",
    "Maple",
    "Pine",
    "Elm",
    "Park",
    "Washington",
    "Lake",
    "Sunset",
    "Highland",
    "Riverside",
]
PERSONAL_EMAIL_DOMAIN = "mail.example"
WORK_EMAIL_DOMAIN = "company.example"

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
]


@dataclass
class EmployeeBase:
    employee_id: str
    hire_date: str
    date_of_birth: str
    gender: str
    first_name: str
    last_name: str
    full_name: str
    org_unit: str
    role_family: str
    location: str
    personal_email: str
    work_email: str
    phone_number: str
    home_address_line1: str
    home_address_city: str
    home_address_state: str
    home_address_postal_code: str
    tax_id: str


def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def choose_manager(employee_ids: List[str], employee_id: str) -> str:
    managers = [eid for eid in employee_ids if eid != employee_id]
    return random.choice(managers)


def write_csv(path: Path, rows: List[Dict], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def pick_name_and_gender() -> tuple[str, str, str, str]:
    gender = random.choice(GENDERS)
    if gender == "F":
        first = random.choice(FIRST_NAMES_F)
    elif gender == "M":
        first = random.choice(FIRST_NAMES_M)
    else:
        first = random.choice(FIRST_NAMES_X)
    last = random.choice(LAST_NAMES)
    return gender, first, last, f"{first} {last}"


def generate_dob_for_hire(hire_date: date) -> str:
    # Age range chosen for plausible adult hiring; avoids unrealistic teen hires.
    age_years = random.randint(22, 60)
    # Add a little day jitter so DOB isn't perfectly aligned to year boundaries.
    day_jitter = random.randint(-120, 120)
    dob = hire_date - timedelta(days=age_years * 365 + day_jitter)
    return dob.isoformat()


def city_to_state_and_country(city: str) -> tuple[str, str]:
    # Simple mapping aligned with the generator's allowed city values.
    if city in {"Seattle", "Austin", "New York"}:
        return {"Seattle": "WA", "Austin": "TX", "New York": "NY"}[city], "US"
    if city == "London":
        return "ENG", "UK"
    return "SG", "SG"


def generate_phone_for_city(city: str) -> str:
    state, country = city_to_state_and_country(city)
    if country == "US":
        return f"+1-415-555-{random.randint(1000, 9999)}"
    if country == "UK":
        return f"+44-20-7946-{random.randint(1000, 9999)}"
    # SG
    return f"+65-6{random.randint(100000, 999999)}"


def generate_postal_for_city(city: str) -> str:
    _, country = city_to_state_and_country(city)
    if country == "US":
        return str(random.randint(10000, 99999))
    if country == "UK":
        # Simplified alphanumeric UK-like code
        letters = "".join(random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ") for _ in range(2))
        digits = f"{random.randint(0, 99):02d}"
        return f"{letters}{digits}"
    return f"{random.randint(100000, 999999)}"


def generate_address_for_city(city: str) -> tuple[str, str, str, str]:
    state, _country = city_to_state_and_country(city)
    street_name = random.choice(STREET_NAMES)
    street_number = random.randint(100, 9999)
    line1 = f"{street_number} {street_name} St"
    postal = generate_postal_for_city(city)
    return line1, city, state, postal


def generate_tax_id() -> str:
    # Synthetic "SSN-like" 9 digit string; keep as string for leading zeros.
    return f"{random.randint(0, 999999999):09d}"


def generate_emails(first_name: str, last_name: str) -> tuple[str, str]:
    f = first_name.lower()
    l = last_name.lower()
    # Avoid edge cases where first/last might be short.
    work = f"{f}.{l}@{WORK_EMAIL_DOMAIN}"
    personal = f"{f}{random.randint(1, 99)}.{l}@{PERSONAL_EMAIL_DOMAIN}"
    return personal, work


def main() -> None:
    random.seed(42)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=89)  # 3 months ~= 90 days

    num_employees = 120
    employee_ids = [f"E{1000 + i}" for i in range(num_employees)]
    base_rows: List[EmployeeBase] = []
    for eid in employee_ids:
        org = random.choice(ORG_UNITS)
        hire_date = start_date - timedelta(days=random.randint(30, 1000))
        gender, first, last, full = pick_name_and_gender()
        personal_email, work_email = generate_emails(first, last)
        location = random.choice(["Seattle", "Austin", "New York", "London", "Singapore"])
        phone = generate_phone_for_city(location)
        address_line1, addr_city, addr_state, addr_postal = generate_address_for_city(location)
        tax_id = generate_tax_id()
        base_rows.append(
            EmployeeBase(
                employee_id=eid,
                hire_date=hire_date.isoformat(),
                date_of_birth=generate_dob_for_hire(hire_date),
                gender=gender,
                first_name=first,
                last_name=last,
                full_name=full,
                org_unit=org,
                role_family=f"{org}_{random.choice(ROLE_FAMILY_SUFFIXES)}",
                location=location,
                personal_email=personal_email,
                work_email=work_email,
                phone_number=phone,
                home_address_line1=address_line1,
                home_address_city=addr_city,
                home_address_state=addr_state,
                home_address_postal_code=addr_postal,
                tax_id=tax_id,
            )
        )

    state: Dict[str, Dict] = {}
    for b in base_rows:
        state[b.employee_id] = {
            "status": "active",
            "manager_id": choose_manager(employee_ids, b.employee_id),
            "team": random.choice(TEAMS[b.org_unit]),
            "salary_band": random.choice(SALARY_BANDS[:3]),
            "fte_flag": random.choice([0, 1, 1, 1]),
            "work_mode": random.choice(WORK_MODES),
            "org_unit": b.org_unit,
            "role_family": b.role_family,
            "location": b.location,
        }

    snapshots: List[Dict] = []
    events: List[Dict] = []

    for snap_date in daterange(start_date, end_date):
        weekday = snap_date.weekday()
        is_monday = weekday == 0
        month_start = snap_date.day <= 3

        for b in base_rows:
            eid = b.employee_id
            row_state = state[eid].copy()

            # Daily mutation probabilities with simple seasonality.
            p_status = 0.010 + (0.007 if is_monday else 0.0)
            p_manager = 0.004 + (0.003 if month_start else 0.0)
            p_team = 0.005
            p_salary = 0.003 + (0.004 if month_start else 0.0)
            p_work_mode = 0.002
            p_org_transfer = 0.002 + (0.001 if month_start else 0.0)
            p_location_change = 0.003 + (0.001 if month_start else 0.0)
            p_role_family_change = 0.002 + (0.001 if month_start else 0.0)
            p_fte_change = 0.001

            previous = row_state.copy()
            status = row_state["status"]

            if status == "active":
                if random.random() < p_status:
                    status = random.choices(["on_leave", "terminated"], weights=[7, 3], k=1)[0]
            elif status == "on_leave":
                if random.random() < 0.25:
                    status = "active"
                elif random.random() < 0.03:
                    status = "terminated"
            else:  # terminated
                if random.random() < 0.02:
                    status = "active"  # rehire

            row_state["status"] = status

            # Only mutable employment attributes when not terminated.
            if status != "terminated":
                # On leave: reduce changes for some fields.
                on_leave_factor = 0.55 if status == "on_leave" else 1.0

                if random.random() < p_manager * on_leave_factor:
                    row_state["manager_id"] = choose_manager(employee_ids, eid)
                if random.random() < p_team * on_leave_factor:
                    row_state["team"] = random.choice(TEAMS[row_state["org_unit"]])
                if random.random() < p_salary * on_leave_factor:
                    idx = SALARY_BANDS.index(row_state["salary_band"])
                    row_state["salary_band"] = SALARY_BANDS[min(len(SALARY_BANDS) - 1, idx + 1)]
                if random.random() < p_work_mode * on_leave_factor:
                    row_state["work_mode"] = random.choice(WORK_MODES)
                if random.random() < p_fte_change * on_leave_factor:
                    row_state["fte_flag"] = random.choice([0, 1])

                # Transfer scenarios: org, team, and role family often change together.
                if random.random() < p_org_transfer:
                    new_org = random.choice([x for x in ORG_UNITS if x != row_state["org_unit"]])
                    row_state["org_unit"] = new_org
                    row_state["team"] = random.choice(TEAMS[new_org])
                    row_state["role_family"] = f"{new_org}_{random.choice(ROLE_FAMILY_SUFFIXES)}"
                    row_state["manager_id"] = choose_manager(employee_ids, eid)

                # Location and role family can change independently.
                if random.random() < p_location_change * on_leave_factor:
                    row_state["location"] = random.choice(["Seattle", "Austin", "New York", "London", "Singapore"])
                if random.random() < p_role_family_change * on_leave_factor:
                    row_state["role_family"] = f"{row_state['org_unit']}_{random.choice(ROLE_FAMILY_SUFFIXES)}"

                # Promotion correlation: promotion often changes manager too.
                if previous["salary_band"] != row_state["salary_band"] and random.random() < 0.65:
                    row_state["manager_id"] = choose_manager(employee_ids, eid)
            else:
                # When terminated, keep employment attributes stable until a rehire happens.
                pass

            state[eid] = row_state

            for field in TRACKED_FIELDS:
                if previous[field] != row_state[field]:
                    events.append(
                        {
                            "employee_id": eid,
                            "event_date": snap_date.isoformat(),
                            "event_type": f"{field}_changed",
                            "attribute_name": field,
                            "previous_value": previous[field],
                            "new_value": row_state[field],
                        }
                    )

            snapshots.append(
                {
                    "snapshot_date": snap_date.isoformat(),
                    "employee_id": eid,
                    "status": row_state["status"],
                    "manager_id": row_state["manager_id"],
                    "team": row_state["team"],
                    "salary_band": row_state["salary_band"],
                    "fte_flag": row_state["fte_flag"],
                    "work_mode": row_state["work_mode"],
                    "org_unit": row_state["org_unit"],
                    "role_family": row_state["role_family"],
                    "location": row_state["location"],
                }
            )

    write_csv(
        RAW_DIR / "employees_base.csv",
        [asdict(x) for x in base_rows],
        [
            "employee_id",
            "hire_date",
            "date_of_birth",
            "gender",
            "first_name",
            "last_name",
            "full_name",
            "org_unit",
            "role_family",
            "location",
            "personal_email",
            "work_email",
            "phone_number",
            "home_address_line1",
            "home_address_city",
            "home_address_state",
            "home_address_postal_code",
            "tax_id",
        ],
    )
    write_csv(
        RAW_DIR / "employee_daily_snapshot.csv",
        snapshots,
        [
            "snapshot_date",
            "employee_id",
            "status",
            "manager_id",
            "team",
            "salary_band",
            "fte_flag",
            "work_mode",
            "org_unit",
            "role_family",
            "location",
        ],
    )
    write_csv(
        RAW_DIR / "employee_change_events.csv",
        events,
        [
            "employee_id",
            "event_date",
            "event_type",
            "attribute_name",
            "previous_value",
            "new_value",
        ],
    )

    # Real-world pattern: a single raw extract file (captured from a source system)
    # is landed to ADLS, and downstream Silver normalizes it into tables.
    # This wide extract keeps raw PII and daily HR fields in one place for practice.
    base_by_id: Dict[str, Dict] = {b.employee_id: asdict(b) for b in base_rows}
    combined_rows: List[Dict] = []
    for s in snapshots:
        eid = s["employee_id"]
        b = base_by_id[eid]
        combined_rows.append(
            {
                "employee_id": eid,
                "snapshot_date": s["snapshot_date"],
                "status": s["status"],
                "manager_id": s["manager_id"],
                "team": s["team"],
                "salary_band": s["salary_band"],
                "fte_flag": s["fte_flag"],
                "work_mode": s["work_mode"],
                "org_unit": s["org_unit"],
                "role_family": s["role_family"],
                "location": s["location"],
                # Raw HR identity / PII
                "hire_date": b["hire_date"],
                "date_of_birth": b["date_of_birth"],
                "gender": b["gender"],
                "full_name": b["full_name"],
                "personal_email": b["personal_email"],
                "work_email": b["work_email"],
                "phone_number": b["phone_number"],
                "home_address_line1": b["home_address_line1"],
                "home_address_city": b["home_address_city"],
                "home_address_state": b["home_address_state"],
                "home_address_postal_code": b["home_address_postal_code"],
                "tax_id": b["tax_id"],
            }
        )

    combined_fieldnames = [
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
    write_csv(
        RAW_DIR / "employee_hr_raw_extract.csv",
        combined_rows,
        combined_fieldnames,
    )
    write_csv(
        ADLS_DIR / "raw" / "employee_hr_raw_extract.csv",
        combined_rows,
        combined_fieldnames,
    )
    print(f"Generated {len(base_rows)} employees, {len(snapshots)} snapshots, {len(events)} events.")


if __name__ == "__main__":
    main()
