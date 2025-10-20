#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convert municipios_acs_s2301_2010_2023.json (long) → wide format
mirroring prm-est2010_2024.json, and append Puerto Rico totals.

Outputs:
  municipios_acs_s2301_2010_2023_wide.json

Structure:
  - One record per municipio (78) + one Puerto Rico total
  - Includes:
      * Unemployment rate (%)
      * Employment-to-population ratio (%)
      * Labor force participation rate (%)
  - Yearly columns 2010–2023
  - Change_2022_2023, Pct_Change_2022_2023
  - Cum_Change_2010_2023, Cum_Pct_Change_2010_2023
"""

import json
import pandas as pd
from pathlib import Path

# ---------- Paths ----------
REPO = Path(__file__).resolve().parents[0]  # adjust if needed
INPUT = REPO / "municipios_acs_s2301_2010_2023.json"
OUTPUT = REPO / "municipios_acs_s2301_2010_2023_wide.json"

# ---------- Load ----------
df = pd.read_json(INPUT)

# Clean municipality names
df["Municipio"] = df["municipio"].str.replace(" Municipio", "", regex=False)

# Keep only needed columns
df = df[[
    "Municipio", "year",
    "unemployment_rate_pct",
    "emp_pop_ratio_pct",
    "labor_force_participation_pct"
]]

# ---------- Helper: pivot one metric ----------
def pivot_metric(df, metric_col, prefix):
    pivoted = df.pivot(index="Municipio", columns="year", values=metric_col)
    pivoted.columns = [f"{prefix}_{int(c)}" for c in pivoted.columns]
    return pivoted

# ---------- Pivot all metrics ----------
unemp = pivot_metric(df, "unemployment_rate_pct", "Unemp")
emp = pivot_metric(df, "emp_pop_ratio_pct", "EmpPop")
lfp = pivot_metric(df, "labor_force_participation_pct", "LaborForce")

# ---------- Combine ----------
merged = unemp.join(emp).join(lfp)
merged.reset_index(inplace=True)

# ---------- Compute changes (for Unemployment Rate) ----------
merged["Change_2022_2023"] = merged["Unemp_2023"] - merged["Unemp_2022"]
merged["Pct_Change_2022_2023"] = (
    merged["Change_2022_2023"] / merged["Unemp_2022"] * 100
)
merged["Cum_Change_2010_2023"] = merged["Unemp_2023"] - merged["Unemp_2010"]
merged["Cum_Pct_Change_2010_2023"] = (
    merged["Cum_Change_2010_2023"] / merged["Unemp_2010"] * 100
)

# ---------- Create Puerto Rico totals (average across municipios) ----------
total_row = merged.copy().drop(columns="Municipio").mean(numeric_only=True)
total_row = total_row.to_dict()
total_row["Municipio"] = "Puerto Rico"

# Append to top (like your population JSON)
merged = pd.concat([
    pd.DataFrame([total_row]),
    merged
], ignore_index=True)

# ---------- Round and export ----------
for col in merged.columns:
    if merged[col].dtype.kind in "fc":
        merged[col] = merged[col].round(2)

records = merged.to_dict(orient="records")

with open(OUTPUT, "w", encoding="utf-8") as f:
    json.dump(records, f, indent=2, ensure_ascii=False)

print(f"✅ Saved {len(records)} records (including Puerto Rico) to {OUTPUT.name}")
