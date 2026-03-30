---
name: deterministic-pii-masking
description: Adds deterministic masking/tokenization for PII in HR datasets and guides how to enforce security boundaries across Bronze/Silver/Gold. Use when user asks about masking techniques, stable pseudonyms, row/column security, or keeping joins working without exposing raw PII.
---
# TL;DR
This skill designs a deterministic masking strategy so PII stays restricted while analytics can still join and aggregate reliably.

# Deterministic PII Masking Skill

## Trigger scenarios
Use this skill when prompts include:
- email masking
- phone masking
- deterministic masking
- tokenization
- masking columns in BI
- PII security in lakehouse

## Workflow
1. Identify PII columns and their sensitivity:
   - raw PII columns (restrict access in Bronze)
   - analytics-facing masked/tokenized columns (promote to Gold)
2. Choose deterministic strategy:
   - stable masked output rules (pattern masking)
   - or stable token/hash (for join compatibility)
3. Ensure deterministic join keys:
   - masked/token columns should be consistent for the same employee across time
4. Implement layer boundaries:
   - Bronze: unmasked PII (RBAC restricted)
   - Silver/Gold: only masked/tokenized columns
5. Document the masking contract:
   - what each masked column contains
   - what it cannot be used for (e.g., exact recovery)

## Output format
- A list of PII columns -> masking approach -> target layer placement
- A small “join compatibility” note for BI modeling
