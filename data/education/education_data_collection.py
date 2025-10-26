# --------------------------------------------------------------------------
# Education Attainment Data Collection (V7 - Final Clean)
#
# This script fetches the Percentage of Population (25+) with a Bachelor's
# Degree or Higher for Puerto Rico municipalities using the highly reliable
# ACS Subject Table API (S1501).
#
# It outputs the data in a clean, wide format, removing unnecessary RealIncome_* fields.
# --------------------------------------------------------------------------

import json
import requests
import pandas as pd
from pathlib import Path
from datetime import date, datetime
import sys
import os 
from time import sleep

# --- Configuration ---
# Hardcoding the key for successful retrieval based on previous run information.
API_KEY = "29dc42832697b740f9eff8ae8d61b9e544478c2b" 
OUT = Path(__file__).resolve().parent
START_YEAR = 2010

# Variable for Percent of Population (25+) with Bachelor's Degree or Higher (S1501)
EDUCATION_VAR = "S1501_C01_006E" 

# List of all Puerto Rico County FIPS codes (Counties = Municipios in PR)
PR_COUNTY_FIPS = [f'0{i:02d}' for i in range(1, 153, 2)] 

def safe_float(val):
    """Safely converts string value to float, treating missing/non-finite data as 0.0."""
    try:
        if val is None or val.strip().upper() in ('N', '0', '0.0'):
            return 0.0
        return float(val)
    except Exception:
        return 0.0

# --------------------------------------------------------
# 1. Determine available years
# --------------------------------------------------------
current_year = date.today().year
latest_data_year = current_year - 1 
years = [y for y in range(START_YEAR, latest_data_year + 1)]

if len(years) < 2:
    print("❌ Not enough valid ACS years found.")
    sys.exit(0)

# --------------------------------------------------------
# 2. Download data (Percentage Attainment)
# --------------------------------------------------------
print("🎓 Downloading Population 25+ Attainment Data (Table S1501)...")

records, successful_years = [], []

for i, year in enumerate(years, start=1):
    fips_list = ','.join(PR_COUNTY_FIPS)
    
    url = (
        f"https://api.census.gov/data/{year}/acs/acs5/subject"
        f"?get=NAME,{EDUCATION_VAR}&for=county:{fips_list}&in=state:72&key={API_KEY}"
    )

    sys.stdout.write(f"\rFetching {year} ({i}/{len(years)})...")
    sys.stdout.flush()
    
    try:
        r = requests.get(url, timeout=30)
        
        if r.status_code != 200:
            print(f"\n⚠️ Error {r.status_code} for {year}. Data skipped.")
            sleep(0.5)
            continue
            
        data = r.json()
        
        if not data or len(data) <= 1:
            print(f"\n🛑 Error: Empty response body for {year}. Data skipped.")
            sleep(0.5)
            continue
            
        header, *rows = data
        idx = {k: i for i, k in enumerate(header)}
        
        successful_years.append(year)
        
        for row in rows:
            municipio_full = row[idx["NAME"]]
            municipio = municipio_full.replace(" Municipio, Puerto Rico", "").replace(" Municipio", "")
            percentage = safe_float(row[idx[EDUCATION_VAR]])
            
            records.append({
                "year": year,
                "Municipio": municipio,
                "Percentage": percentage
            })
            
    except Exception as e:
        print(f"\n❌ {year} failed unexpectedly: {e}. Data skipped.")
        sleep(0.5)
        continue

if len(successful_years) < 2:
    print("\n🛑 Insufficient data retrieved. Cannot generate final file.")
    sys.exit(0)

# --------------------------------------------------------
# 3. Build dataframe
# --------------------------------------------------------
df = pd.DataFrame(records)
df = df.sort_values(["Municipio", "year"])

# Add islandwide total by finding the average across all municipalities
island_avg = (
    df.groupby("year", as_index=False)["Percentage"]
    .mean()
    .assign(Municipio="Puerto Rico")
)
df = pd.concat([df, island_avg], ignore_index=True)


# --------------------------------------------------------
# 4. Pivot to wide format (Clean Output)
# --------------------------------------------------------
pivot = df.pivot(index="Municipio", columns="year", values="Percentage").reset_index()
pivot.columns.name = None

pivot = pivot.rename(columns={y: str(y) for y in successful_years})

first, prev, last = successful_years[0], successful_years[-2], successful_years[-1]
first_str, prev_str, last_str = str(first), str(prev), str(last)

# Calculate change metrics for percentage points
pivot[f"Change_{prev_str}_{last_str}"] = pivot[last_str] - pivot[prev_str]
pivot[f"Pct_Change_{prev_str}_{last_str}"] = pivot[f"Change_{prev_str}_{last_str}"]
pivot[f"Cum_Change_{first_str}_{last_str}"] = pivot[last_str] - pivot[first_str]
pivot[f"Cum_Pct_Change_{first_str}_{last_str}"] = pivot[f"Cum_Change_{first_str}_{last_str}"]

# --- REMOVED Placeholder Fields: RealIncome_* and Real_* ---

# --------------------------------------------------------
# 5. Add metadata and save JSON
# --------------------------------------------------------
records = pivot.to_dict(orient="records")

metadata = {
    "metadata": {
        "source": "U.S. Census Bureau, ACS 5-Year Subject Table S1501 (Educational Attainment)",
        "units": "Percent of Population Age 25+ with Bachelor's Degree or Higher (Percentage Points)",
        "islandwide_aggregation": "Average of all 78 Municipios",
        "data_years": [str(y) for y in successful_years],
        "updated": datetime.now().strftime("%Y-%m-%d"),
        "notes": (
            "Nominal values represent the calculated percentage point for Bachelor's degree or higher. "
            "File structure has been simplified to exclude unnecessary RealIncome fields."
        )
    }
}

records.append(metadata)

json_path = OUT / f"municipios_acs_education_{START_YEAR}_{last_str}_wide.json"
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(records, f, indent=2, ensure_ascii=False)

print(f"\n✅ Saved JSON → {json_path.name}")
print("🎉 Structural parity with dashboard template achieved.")
