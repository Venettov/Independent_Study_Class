#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
employment_establishment_data_collection.py — Final API Version
(Directly fetches data from Census CBP and outputs JSON identical in
structure to municipios_acs_s1901_median_income_2010_2023_wide.json)
"""

import json
import requests
import pandas as pd
from pathlib import Path
from datetime import date, datetime
import sys

API_KEY = "29dc42832697b740f9eff8ae8d61b9e544478c2b"
OUT = Path(__file__).resolve().parent
START_YEAR = 2010

def safe_int(val):
    try:
        if val in ('N', '0'):
            return 0
        return int(val)
    except Exception:
        return None

def get_naics_variable_name(year):
    if year >= 2017: return "NAICS2017"
    elif year >= 2012: return "NAICS2012"
    elif year >= 2007: return "NAICS2007"
    return "NAICS2002"

# --------------------------------------------------------
# 1. Determine available CBP years
# --------------------------------------------------------
current_year = date.today().year
years = [y for y in range(START_YEAR, current_year)]
if not years:
    raise RuntimeError("❌ No valid CBP years found.")

# --------------------------------------------------------
# 2. Download data
# --------------------------------------------------------
print("📊 Downloading Total Employment from CBP...")
VARS = ["NAME", "EMP"]
records, successful_years = [], []

for i, year in enumerate(years, start=1):
    naics_var = get_naics_variable_name(year)
    url = (
        f"https://api.census.gov/data/{year}/cbp"
        f"?get={','.join(VARS)}&for=county:*&in=state:72&{naics_var}=00&key={API_KEY}"
    )
    sys.stdout.write(f"\rFetching {year} ({i}/{len(years)})...")
    sys.stdout.flush()
    try:
        r = requests.get(url, timeout=60)
        if r.status_code != 200:
            print(f"\n⚠️ Error {r.status_code} for {year}")
            continue
        data = r.json()
        header, *rows = data
        idx = {k: i for i, k in enumerate(header)}
        if not rows: continue
        successful_years.append(year)
        for row in rows:
            municipio = row[idx["NAME"]].replace(" Municipio, Puerto Rico", "")
            if municipio == "Puerto Rico":
                continue
            records.append({
                "year": year,
                "Municipio": municipio,
                "employment": safe_int(row[idx["EMP"]])
            })
    except Exception as e:
        print(f"\n❌ {year} failed: {e}")
        continue

if len(successful_years) < 2:
    print("🛑 Need at least two valid years.")
    sys.exit(0)

# --------------------------------------------------------
# 3. Build dataframe
# --------------------------------------------------------
df = pd.DataFrame(records)
df = df.sort_values(["Municipio", "year"]).dropna(subset=["employment"])

# Add islandwide total
island = (
    df.groupby("year", as_index=False)["employment"]
    .sum()
    .assign(Municipio="Puerto Rico")
)
df = pd.concat([df, island], ignore_index=True)

# --------------------------------------------------------
# 4. Pivot to wide format
# --------------------------------------------------------
pivot = df.pivot(index="Municipio", columns="year", values="employment").reset_index()
pivot.columns.name = None
pivot = pivot.rename(columns={y: str(y) for y in successful_years})

first, prev, last = successful_years[0], successful_years[-2], successful_years[-1]

pivot[f"Change_{prev}_{last}"] = pivot[str(last)] - pivot[str(prev)]
pivot[f"Pct_Change_{prev}_{last}"] = (pivot[f"Change_{prev}_{last}"] / pivot[str(prev)]) * 100
pivot[f"Cum_Change_{first}_{last}"] = pivot[str(last)] - pivot[str(first)]
pivot[f"Cum_Pct_Change_{first}_{last}"] = (pivot[f"Cum_Change_{first}_{last}"] / pivot[str(first)]) * 100

# Add RealIncome_* placeholders
for y in successful_years:
    pivot[f"RealIncome_{y}"] = None

for key in [
    f"Real_Change_{prev}_{last}",
    f"Real_Pct_Change_{prev}_{last}",
    f"Real_Cum_Change_{first}_{last}",
    f"Real_Cum_Pct_Change_{first}_{last}",
]:
    pivot[key] = None

# --------------------------------------------------------
# 5. Add metadata and save JSON
# --------------------------------------------------------
records = pivot.to_dict(orient="records")

metadata = {
    "metadata": {
        "source": "U.S. Census Bureau, County Business Patterns (CBP), NAICS 00 (All Industries)",
        "units": "Number of Paid Employees (as of March 12)",
        "islandwide_aggregation": True,
        "data_years": [str(y) for y in successful_years],
        "updated": datetime.now().strftime("%Y-%m-%d"),
        "notes": (
            "Nominal employment values represent total paid employees. "
            "RealIncome_* and Real_* fields are null placeholders for "
            "compatibility with municipios_acs_s1901_median_income_2010_2023_wide.json."
        )
    }
}

records.append(metadata)

json_path = OUT / f"municipios_cbp_total_employment_{START_YEAR}_{last}_wide.json"
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(records, f, indent=2, ensure_ascii=False)

print(f"\n✅ Saved JSON → {json_path.name}")
print("🎉 Structural parity with income JSON achieved.")
