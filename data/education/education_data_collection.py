# --------------------------------------------------------------------------
# Education Attainment Data Collection (V8 - FIPS & Naming Fixes)
#
# This script fetches the Percentage of Population (25+) with a Bachelor's
# Degree or Higher for Puerto Rico municipalities using the highly reliable
# ACS Subject Table API (S1501).
#
# FIXES:
# 1. Ensures all 78 FIPS codes are explicitly listed to prevent missing Municipios (like Yauco/153).
# 2. Adjusts START_YEAR to 2013 for better data coverage.
# 3. Improves municipality name sanitization to handle various API return formats.
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
# FIX: Start year adjusted to 2013 for reliable data coverage across all 78 municipios
START_YEAR = 2013

# Variable for Percent of Population (25+) with Bachelor's Degree or Higher (S1501)
EDUCATION_VAR = "S1501_C01_006E" 

# FIX: Explicit list of all 78 Puerto Rico County FIPS codes (Counties = Municipios in PR)
# FIPS 001 to 153, odd numbers only. This ensures Yauco (153) is included.
PR_COUNTY_FIPS = [
    '001', '003', '005', '007', '009', '011', '013', '015', '017', '019', '021', '023',
    '025', '027', '029', '031', '033', '035', '037', '039', '041', '043', '045', '047',
    '049', '051', '053', '054', '055', '057', '059', '061', '063', '065', '067', '069',
    '071', '073', '075', '077', '079', '081', '083', '085', '087', '089', '091', '093',
    '095', '097', '099', '101', '103', '105', '107', '109', '111', '113', '115', '117',
    '119', '121', '123', '125', '127', '129', '131', '133', '135', '137', '139', '141',
    '143', '145', '147', '149', '151', '153' # Added FIPS 153 (Yauco)
]

def safe_float(val):
    """Safely converts string value to float, treating missing/non-finite data as 0.0."""
    try:
        if val is None or str(val).strip().upper() in ('N', '-', '0', '0.0', '(X)', 'NA'):
            return 0.0
        return float(val)
    except Exception:
        return 0.0

def clean_municipio_name(full_name):
    """Strips common Census suffixes to get just the Municipio name."""
    name = str(full_name).replace(", Puerto Rico", "").strip()
    name = name.replace(" Municipio", "").strip()
    return name

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
        
        if len(rows) < 78:
            # We expect 78 municipios + 1 header row. If fewer than 78 rows, log a warning.
            print(f"\n⚠️ Only {len(rows)} municipios retrieved for {year}. Data may be incomplete.")

        successful_years.append(year)
        
        for row in rows:
            municipio_full = row[idx["NAME"]]
            # FIX: Use improved cleaning function
            municipio = clean_municipio_name(municipio_full)
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
# Use the corrected municipality name to group and sort
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
