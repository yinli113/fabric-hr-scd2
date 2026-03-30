#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import re
from datetime import date


MASK_SALT = "employee-scd2-lab-static-salt"


def sha256_token(value: str) -> str:
    return hashlib.sha256(f"{MASK_SALT}|{value}".encode("utf-8")).hexdigest()


def mask_email_deterministic(email: str) -> str:
    # Example: firstname.lastname@company.com -> f***e@company.com
    if not email or "@" not in email:
        return "REDACTED_EMAIL"
    local, domain = email.split("@", 1)
    local_clean = re.sub(r"[^a-zA-Z0-9]", "", local)
    if len(local_clean) <= 2:
        return f"{local_clean[:1]}***@{domain}"
    return f"{local_clean[0]}***{local_clean[-1]}@{domain}"


def mask_phone_deterministic(phone: str) -> str:
    # Example: +1-415-555-1234 -> +1-***-***-1234 (keeps last 4 digits)
    digits = re.sub(r"[^0-9+]", "", phone)
    all_digits = re.sub(r"[^0-9]", "", digits)
    if len(all_digits) < 4:
        return "REDACTED_PHONE"
    last4 = all_digits[-4:]
    prefix = "+" if digits.strip().startswith("+") else ""
    # Keep a generic masked body while preserving last4.
    return f"{prefix}{'***'}{'***'}{'-'}{last4}"


def mask_postal_deterministic(postal: str) -> str:
    # Keeps only last 2-3 chars; supports alphanumeric postals.
    if not postal:
        return "REDACTED_POSTAL"
    s = str(postal).replace(" ", "")
    keep = s[-3:] if len(s) >= 3 else s[-1:]
    return f"***{keep}"


def mask_full_name_deterministic(full_name: str) -> str:
    if not full_name:
        return "REDACTED_NAME"
    parts = [p for p in re.split(r"\s+", full_name.strip()) if p]
    first = parts[0]
    last = parts[-1]
    first_initial = first[0].upper() if first else "X"
    last_initial = last[0].upper() if last else "X"
    return f"{first_initial}*** {last_initial}."


def birth_year_deterministic(dob_iso: str) -> str:
    # Converts YYYY-MM-DD -> YYYY, which is often enough for HR analytics without exposing full DOB.
    try:
        parts = dob_iso.split("-")
        return parts[0]  # YYYY
    except Exception:
        return "REDACTED_BIRTHYEAR"

