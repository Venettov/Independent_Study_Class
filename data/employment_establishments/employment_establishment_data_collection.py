#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
employment_establishment_data_collection.py

Collects Total Employment (number of paid employees) for Puerto Rico municipios
(2010–latest available) from the County Business Patterns (CBP).

Outputs:
- municipios_cbp_total_employment_2010_XXXX.csv
- municipios_cbp_total_employment_2010_XXXX_wide.json
  (structurally identical to municipios_acs_s1901_median_income_2010_2023_wide.json)

Author: Modified by Andres Ruiz
Updated: Adds null-valued RealIncome fields for structural parity with income JSON.
"""

import json
import requests
import pandas as pd
from pathlib import Path
import sys
from datetime import date, datetime

# =============================
# CONFIGURATION
# =============================

API_KEY = "29dc42832697b740f9eff8ae8d61b9e544478c2b"  # <-- Your Census API key
OUT = Path(__file__).resolve().parent
START_YEAR = 2010

# =============================
# HELPER FUNCTIONS
# =============================

def safe_int(val):
    """Convert value to int, handling non-numeric or suppressed ('N') data."""
    try:
        if val in ('N', '0'):
            return 0
        return int(val)
    except Exception:
        return None


def get_naics_variable_name(year):
    """Return correct NAICS variable name based on data year."""
    if year >= 2017:
        return "NAICS2017"
    elif year >= 2012:
        return "NAICS2012"
    elif year >= 2007:
        return "NAICS2007"
    return "NAICS2002"


# =============================
# 1. DETERMINE AVAILABLE YEARS
# =============================

print("\n🧭 Checking potential CBP data years...")
current_data_check_year = date.today().year - 1
years = [y for y in range(START_YEAR, current_data_check_year)]

if not years:
    raise RuntimeError("❌ No valid CBP years detected.")
print(f"✅ Potential CBP years to check: {years}")

# =============================
# 2. DOWNLOAD CBP DATA
# =============================

print("\n🧑‍💼 Downloading Total Employment (CBP)...")

VARS = ["NAME", "EMP"]
records = []
successful_years = []

for i, year in enumerate(years, start=1):
    naics_var = get_naics_variable_name(year)
    url = (
        f"https://api.census.gov/data/{year}/cbp"
        f"?get={','.join(VARS)}&for=county:*&in=state:72&{naics_var}=00&key={API_KEY}"
    )
    sys.stdout.write(f"\rFetching {year} ({i}/{len(years)}) using {naics_var}... ")
    sys.stdout.flush()
    try:
        r = requests.get(url, timeout=60)

        if r.status_code != 200:
            print(f"\n⚠️  API Error {r.status_code} for {year}: {r.text[:100].strip()}...")
            continue

        data = r.json()
        header, *rows = data
        idx = {k: i for i, k in enumerate(header)}

        if not rows:
            print(f"\n⚠️  No data returned for {year}.")
            continue

        successful_years.append(year)

        for row in rows:
            municipio_name = row[idx["NAME"]].replace(" Municipio, Puerto Rico", "")
            if municipio_name == "Puerto Rico":
                continue
            records.append({
                "year": year,
                "municipio": municipio_name,
                "TotalEmployment": safe_int(row[idx["EMP"]])
            })

    except json.JSONDecodeError as e:
        print(f"\n❌ JSON Decode Error for {year}: {e}")
        continue
    except Exception as e:
        print(f"\n❌ Failed for {year}: {e}")
        continue

print("\n✅ CBP employment download complete.")

if len(successful_years) < 2:
    print("\n🛑 Script stopped. Need at least two years of data.")
    sys.exit(0)

# =============================
# 3. ADD ISLANDWIDE TOTAL
# =============================

df = pd.DataFrame(records).dropna(subset=["TotalEmployment"])
df = df.sort_values(["municipio", "year"]).reset_index(drop=True)

island = (
    df.groupby("year", as_index=False)["TotalEmployment"]
    .sum(numeric_only=True)
    .assign(municipio="Puerto Rico")
)
df = pd.concat([df, island], ignore_index=True)
print("✅ Added islandwide total for Puerto Rico.")

# =============================
# 4. SAVE LONG CSV
# =============================

cpi_ref_year = max(successful_years)
csv_path = OUT / f"municipios_cbp_total_employment_{START_YEAR}_{cpi_ref_year}.csv"
df.to_csv(csv_path, index=False)
print(f"✅ Saved long CSV → {csv_path.name}")

# =============================
# 5. BUILD WIDE JSON (HTML-Compatible)
# =============================

pivot_nom = df.pivot(index="municipio", columns="year", values="TotalEmployment").reset_index()
pivot_nom.columns.name = None
pivot_nom = pivot_nom.rename(columns={"municipio": "Municipio"})
years_str = [str(y) for y in successful_years]
pivot_nom = pivot_nom.rename(columns={y: str(y) for y in successful_years})

first, prev, last = successful_years[0], successful_years[-2], successful_years[-1]

# Add computed fields
pivot_nom[f"Change_{prev}_{last}"] = pivot_nom[str(last)] - pivot_nom[str(prev)]
pivot_nom[f"Pct_Change_{prev}_{last}"] = (pivot_nom[f"Change_{prev}_{last}"] / pivot_nom[str(prev)]) * 100
pivot_nom[f"Cum_Change_{first}_{last}"] = pivot_nom[str(last)] - pivot_nom[str(first)]
pivot_nom[f"Cum_Pct_Change_{first}_{last}"] = (pivot_nom[f"Cum_Change_{first}_{last}"] / pivot_nom[str(first)]) * 100

# Convert to dict
records = pivot_nom.to_dict(orient="records")

# =============================
# 6. ADD NULL REAL-INCOME FIELDS
# =============================

for rec in records:
    # Add RealIncome_YYYY fields
    for y in years_str:
        rec[f"RealIncome_{y}"] = None

    # Add null real change fields (matching ACS income JSON)
    rec[f"Real_Change_{prev}_{last}"] = None
    rec[f"Real_Pct_Change_{prev}_{last}"] = None
    rec[f"Real_Cum_Change_{first}_{last}"] = None
    rec[f"Real_Cum_Pct_Change_{first}_{last}"] = None

# =============================
# 7. ADD METADATA
# =============================

metadata = {
    "metadata": {
        "source": "U.S. Census Bureau, County Business Patterns (CBP), NAICS 00 (All Industries)",
        "units": "Number of Paid Employees (as of March 12)",
        "islandwide_aggregation": "Sum of all Municipio employment (Total)",
        "data_years": successful_years,
        "updated": datetime.now().strftime("%Y-%m-%d"),
        "notes": (
            "This dataset mirrors the structure of municipios_acs_s1901_median_income_2010_2023_wide.json. "
            "All 'RealIncome' and 'Real_*' fields are included as null values for structural compatibility. "
            "Employment data represent nominal counts of paid employees (not inflation-adjusted)."
        )
    }
}

records.append(metadata)

# =============================
# 8. SAVE WIDE JSON
# =============================

json_path = OUT / f"municipios_cbp_total_employment_{START_YEAR}_{cpi_ref_year}_wide.json"
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(records, f, ensure_ascii=False, indent=2)

print(f"✅ Saved JSON → {json_path.name}")
print(f"🎉 Employment data ready through {cpi_ref_year} with structural parity achieved.")
