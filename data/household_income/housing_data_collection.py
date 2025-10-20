#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
employment_data_collection.py

Collects employment and unemployment data for Puerto Rico municipios (2010–2023)
from the U.S. Census Bureau ACS 5-year Subject Tables (S2301) and saves to
both CSV and JSON formats.

Also downloads Puerto Rico-wide unemployment data from the BLS Local Area
Unemployment Statistics (LAUS) program via FRED (monthly and annual).

Author: Andres Ruiz
"""

import json
from pathlib import Path
import requests
import pandas as pd
import sys

# =============================
# CONFIGURATION
# =============================

API_KEY = "29dc42832697b740f9eff8ae8d61b9e544478c2b"  # Replace with your Census API key
OUT = Path(__file__).resolve().parent   # Save output in same folder

# =============================
# HELPER FUNCTIONS
# =============================

def safe_float(val):
    """Convert value to float or None if invalid."""
    try:
        return float(val)
    except Exception:
        return None

def clean_cols(df):
    """Normalize column names: lowercase, strip spaces, remove BOM."""
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace("\ufeff", "", regex=False)
    )
    return df

def detect_date_col(cols):
    """Return the most likely date column from a list of names."""
    for c in cols:
        if "date" in c.lower():
            return c
    return None

# =============================
# 1. CENSUS ACS (S2301)
# =============================

print("\n📊 Downloading Census ACS (S2301) data for Puerto Rico municipios...")

VARS = [
    "NAME",
    "S2301_C04_001E", "S2301_C04_001M",  # Unemployment rate (est, MOE)
    "S2301_C03_001E", "S2301_C03_001M",  # Employment-population ratio (est, MOE)
    "S2301_C02_001E", "S2301_C02_001M"   # Labor-force participation (est, MOE)
]

records = []
years = range(2010, 2024)  # 2010–2023 available for ACS 5-year

for i, year in enumerate(years, start=1):
    url = (
        f"https://api.census.gov/data/{year}/acs/acs5/subject"
        f"?get={','.join(VARS)}&for=county:*&in=state:72&key={API_KEY}"
    )
    sys.stdout.write(f"\rFetching {year} ({i}/{len(years)}) ...")
    sys.stdout.flush()

    r = requests.get(url, timeout=60)
    if r.status_code != 200:
        print(f"\n⚠️  Error {r.status_code} for {year}: {r.text[:200]}")
        continue

    data = r.json()
    header, *rows = data
    idx = {k: i for i, k in enumerate(header)}

    for row in rows:
        records.append({
            "year": year,
            "municipio": row[idx["NAME"]].replace(", Puerto Rico", ""),
            "state_fips": row[idx["state"]],
            "county_fips": row[idx["county"]],
            "geoid": f"{row[idx['state']]}{row[idx['county']]}",
            "unemployment_rate_pct": safe_float(row[idx["S2301_C04_001E"]]),
            "unemployment_rate_moe": safe_float(row[idx["S2301_C04_001M"]]),
            "emp_pop_ratio_pct": safe_float(row[idx["S2301_C03_001E"]]),
            "emp_pop_ratio_moe": safe_float(row[idx["S2301_C03_001M"]]),
            "labor_force_participation_pct": safe_float(row[idx["S2301_C02_001E"]]),
            "labor_force_participation_moe": safe_float(row[idx["S2301_C02_001M"]]),
        })

print("\n✅ Census data downloaded successfully.")

if not records:
    raise RuntimeError("No records retrieved from the Census API.")

# Convert to DataFrame
df = pd.DataFrame(records).sort_values(["year", "municipio"]).reset_index(drop=True)

# Save CSV + JSON
csv_path = OUT / "municipios_acs_s2301_2010_2023.csv"
json_path = OUT / "municipios_acs_s2301_2010_2023.json"
df.to_csv(csv_path, index=False)
df.to_json(json_path, orient="records", indent=2, force_ascii=False)
print(f"✅ Saved {csv_path.name} and {json_path.name}")

# =============================
# 2. BLS LAUS (via FRED)
# =============================

print("\n📈 Downloading BLS (FRED) Puerto Rico unemployment data...")

# -----------------------------
# Monthly (PRUR, seasonally adjusted)
# -----------------------------
fred_monthly = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=PRUR"
m = pd.read_csv(fred_monthly)
m = clean_cols(m)
print("Monthly columns detected:", m.columns.tolist())

date_col = detect_date_col(m.columns)
value_col = [c for c in m.columns if c != date_col][0]

m = m.rename(columns={date_col: "date", value_col: "unemployment_rate_pct"})
m["year"] = m["date"].astype(str).str.slice(0, 4).astype(int)
m = m[m["year"] >= 2010]
m_path = OUT / "puertorico_bls_unemployment_2010_2024_monthly.csv"
m.to_csv(m_path, index=False)
print(f"✅ Saved {m_path.name}")

# -----------------------------
# Annual (LAUS)
# -----------------------------
fred_annual = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=LAUST720000000000003A"
a = pd.read_csv(fred_annual)
a = clean_cols(a)
print("Annual columns detected:", a.columns.tolist())

date_col = detect_date_col(a.columns)
value_col = [c for c in a.columns if c != date_col][0]

a = a.rename(columns={date_col: "date", value_col: "unemployment_rate_pct"})
a["year"] = a["date"].astype(str).str.slice(0, 4).astype(int)
a = a[a["year"] >= 2010]
a_path = OUT / "puertorico_bls_unemployment_2010_2024_annual.csv"
a.to_csv(a_path, index=False)
print(f"✅ Saved {a_path.name}")

print("\n🎉 All employment data collected and saved successfully!")
