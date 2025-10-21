#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
employment_establishment_data_collection.py

Collects Total Employment (number of paid employees) for Puerto Rico municipios
(2010–latest available) from the County Business Patterns (CBP).

Outputs:
- municipios_cbp_total_employment_2010_XXXX.csv
- municipios_cbp_total_employment_2010_XXXX_wide.json

Author: Modified from original script by Andres Ruiz
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

API_KEY = "29dc42832697b740f9eff8ae8d61b9e544478c2b"  # <-- YOUR CENSUS API KEY INCLUDED!
OUT = Path(__file__).resolve().parent
START_YEAR = 2010

# =============================
# HELPER FUNCTIONS
# =============================

def safe_int(val):
    """Convert value to int, handling non-numeric, suppressed ('N'), or missing data."""
    try:
        # Treat 'N' (suppressed) and '0' as 0, otherwise convert to integer.
        # This is common in CBP data.
        if val in ('N', '0'):
            return 0
        return int(val)
    except Exception:
        return None

def get_naics_variable_name(year):
    """Returns the correct NAICS variable name based on the data year."""
    if year >= 2017:
        return "NAICS2017"
    elif year >= 2012:
        return "NAICS2012"
    elif year >= 2007:
        return "NAICS2007"
    # Fallback for older years, though 2010 is the start
    return "NAICS2002" 

# =============================
# 1. DETERMINE AVAILABLE YEARS
# =============================

print("\n🧭 Checking potential CBP data years...")
# CBP data is typically lagged. We limit to the year before the current year.
current_data_check_year = date.today().year - 1 
years = [y for y in range(START_YEAR, current_data_check_year)]

if not years:
    raise RuntimeError("❌ No valid CBP years detected.")
print(f"✅ Potential CBP years to check: {years}")

# =============================
# 2. DOWNLOAD CBP (Total Employment - ALL INDUSTRIES)
# =============================

print("\n🧑‍💼 Downloading Total Employment (CBP)...")

# Variables: NAME and EMP (number of paid employees)
# Filter: NAICS code 00 (All sectors/All Industries)
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
        
        # Explicit check for API Key errors or other failures
        if r.status_code != 200:
            # If the key is invalid, the API often returns a 400 with a text error
            print(f"\n⚠️  API Error {r.status_code} for {year}. Check key or try year later. Response snippet: {r.text[:100].strip()}...")
            continue
            
        data = r.json()
        header, *rows = data
        idx = {k: i for i, k in enumerate(header)}
        
        if not rows:
            print(f"\n⚠️  No data returned for {year}. Data may be unavailable or suppressed at this level.")
            continue
            
        successful_years.append(year)
        
        for row in rows:
            # Clean municipio name: e.g., "Adjuntas Municipio, Puerto Rico" -> "Adjuntas Municipio"
            municipio_name = row[idx["NAME"]].replace(" Municipio, Puerto Rico", "")

            # Exclude the full "Puerto Rico" row (state:72) if it appears in the county list
            if municipio_name == "Puerto Rico":
                continue

            records.append({
                "year": year,
                "municipio": municipio_name,
                "TotalEmployment": safe_int(row[idx["EMP"]]) 
            })
            
    except json.JSONDecodeError as e:
        print(f"\n❌ JSON Decode Error for {year}. The API might have returned an HTML or plaintext error. Error: {e}")
        continue
    except Exception as e:
        print(f"\n❌ Failed for {year}: {e}")
        continue

print("\n✅ CBP employment download complete.")

# --- POST-DOWNLOAD PROCESSING ---

if len(successful_years) < 2:
    print("\n\n🛑 Script stopped. Need at least two years of successful data to calculate changes.")
    sys.exit(0)

# Filter the DataFrame to only include successful years
df = pd.DataFrame(records).dropna(subset=["TotalEmployment"])
df = df.sort_values(["municipio", "year"]).reset_index(drop=True)
cpi_ref_year = max(successful_years) 

# =============================
# 3. ADD ISLANDWIDE TOTAL ("Puerto Rico")
# =============================

island = (
    df.groupby("year", as_index=False)["TotalEmployment"]
    .sum(numeric_only=True)
    .assign(municipio="Puerto Rico")
)
df = pd.concat([df, island], ignore_index=True)
print("✅ Added islandwide total row for Puerto Rico.")

# =============================
# 4. SAVE LONG CSV
# =============================

csv_path = OUT / f"municipios_cbp_total_employment_{START_YEAR}_{cpi_ref_year}.csv"
df.to_csv(csv_path, index=False)
print(f"✅ Saved long format CSV → {csv_path.name}")

# =============================
# 5. BUILD WIDE JSON (HTML-compatible)
# =============================

pivot_nom = df.pivot(index="municipio", columns="year", values="TotalEmployment").reset_index()
pivot_nom.columns.name = None

years_str = [str(y) for y in successful_years]
pivot_nom = pivot_nom.rename(columns={y: str(y) for y in successful_years})
wide = pivot_nom.rename(columns={"municipio": "Municipio"})

first, prev, last = successful_years[0], successful_years[-2], successful_years[-1]

# Change calculation for employment
wide[f"Change_{prev}_{last}"] = wide[str(last)] - wide[str(prev)]
wide[f"Pct_Change_{prev}_{last}"] = (wide[f"Change_{prev}_{last}"] / wide[str(prev)]) * 100
wide[f"Cum_Change_{first}_{last}"] = wide[str(last)] - wide[str(first)]
wide[f"Cum_Pct_Change_{first}_{last}"] = (wide[f"Cum_Change_{first}_{last}"] / wide[str(first)]) * 100

records = wide.to_dict(orient="records")

# =============================
# 6. ADD METADATA
# =============================

metadata = {
    "metadata": {
        "source": "U.S. Census Bureau, County Business Patterns (CBP), NAICS 00 (All Industries)",
        "units": "Number of Paid Employees (as of March 12)",
        "islandwide_aggregation": "Sum of all Municipio employment (Total)",
        "data_years": successful_years,
        "updated": datetime.now().strftime("%Y-%m-%d"),
        "notes": (
            "This data represents the total number of paid employees (EMP) for all industries (NAICS 00). "
            "It is a nominal series; no inflation adjustment is applied since units are persons, not dollars."
        )
    }
}

# Append metadata as the final JSON entry
records.append(metadata)

# =============================
# 7. SAVE WIDE JSON
# =============================

json_path = OUT / f"municipios_cbp_total_employment_{START_YEAR}_{cpi_ref_year}_wide.json"
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(records, f, ensure_ascii=False, indent=2)

print(f"✅ Saved HTML-compatible JSON with metadata → {json_path.name}")
print(f"\n🎉 All total employment data (nominal, islandwide, documented) ready through {cpi_ref_year}.")
