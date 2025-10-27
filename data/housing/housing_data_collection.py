# --------------------------------------------------------------------------
# Housing Unit Data Collection
#
# This script fetches the Total Housing Units for all Puerto Rico
# municipalities using the ACS Detailed Table API (B25001).
#
# FIXES:
# 1. Uses ACS table B25001 for Total Housing Units.
# 2. Ensures all 78 FIPS codes are explicitly listed.
# 3. Maintains the original wide-format JSON output structure.
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
# Set start year for reliable ACS 5-year estimates
START_YEAR = 2013

# Variable for Total Housing Units (B25001)
HOUSING_VAR = "B25001_001E" 

# Housing data units are absolute counts, not percentages.
DATA_UNITS = "Total Housing Units (Count)"

# FIX: Explicit list of all 78 Puerto Rico County FIPS codes (Counties = Municipios in PR)
PR_COUNTY_FIPS = [
    '001', '003', '005', '007', '009', '011', '013', '015', '017', '019', '021', '023',
    '025', '027', '029', '031', '033', '035', '037', '039', '041', '043', '045', '047',
    '049', '051', '053', '054', '055', '057', '059', '061', '063', '065', '067', '069',
    '071', '073', '075', '077', '079', '081', '083', '085', '087', '089', '091', '093',
    '095', '097', '099', '101', '103', '105', '107', '109', '111', '113', '115', '117',
    '119', '121', '123', '125', '127', '129', '131', '133', '135', '137', '139', '141',
    '143', '145', '147', '149', '151', '153'
]

def safe_float(val):
    """Safely converts string value to float, treating missing/non-finite data as 0.0."""
    try:
        # Housing units are whole numbers, but float conversion handles missing data cleanly.
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
# 2. Download data (Housing Units)
# --------------------------------------------------------
print("🏠 Downloading Total Housing Units Data (Table B25001)...")

records, successful_years = [], []

for i, year in enumerate(years, start=1):
    fips_list = ','.join(PR_COUNTY_FIPS)
    
    # Using the standard ACS endpoint for detailed table B25001
    url = (
        f"https://api.census.gov/data/{year}/acs/acs5"
        f"?get=NAME,{HOUSING_VAR}&for=county:{fips_list}&in=state:72&key={API_KEY}"
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
            print(f"\n⚠️ Only {len(rows)} municipios retrieved for {year}. Data may be incomplete.")

        successful_years.append(year)
        
        for row in rows:
            municipio_full = row[idx["NAME"]]
            municipio = clean_municipio_name(municipio_full)
            # Use safe_float for data consistency, but values are integer counts
            count = safe_float(row[idx[HOUSING_VAR]]) 
            
            records.append({
                "year": year,
                "Municipio": municipio,
                "Count": count
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
# Rename the column to 'Value' for general purpose calculations/pivoting
df = df.rename(columns={'Count': 'Value'}) 
df = df.sort_values(["Municipio", "year"])

# Add islandwide total by finding the SUM across all municipalities
# Housing units are additive (Total), so we use sum() instead of mean()
island_total = (
    df.groupby("year", as_index=False)["Value"]
    .sum()
    .assign(Municipio="Puerto Rico")
)
df = pd.concat([df, island_total], ignore_index=True)


# --------------------------------------------------------
# 4. Pivot to wide format (Clean Output)
# --------------------------------------------------------
pivot = df.pivot(index="Municipio", columns="year", values="Value").reset_index()
pivot.columns.name = None

pivot = pivot.rename(columns={y: str(y) for y in successful_years})

first, prev, last = successful_years[0], successful_years[-2], successful_years[-1]
first_str, prev_str, last_str = str(first), str(prev), str(last)

# Calculate change metrics (Absolute Change)
pivot[f"Change_{prev_str}_{last_str}"] = pivot[last_str] - pivot[prev_str]
pivot[f"Cum_Change_{first_str}_{last_str}"] = pivot[last_str] - pivot[first_str]

# Calculate Percentage Change (Change / Previous Value)
pivot[f"Pct_Change_{prev_str}_{last_str}"] = (
    (pivot[f"Change_{prev_str}_{last_str}"] / pivot[prev_str]) * 100
).round(2)

# Calculate Cumulative Percentage Change (Cum_Change / First Value)
pivot[f"Cum_Pct_Change_{first_str}_{last_str}"] = (
    (pivot[f"Cum_Change_{first_str}_{last_str}"] / pivot[first_str]) * 100
).round(2)

# Reorder columns to match the output format (Municipio, Years, Changes)
cols = (
    ["Municipio"] 
    + [str(y) for y in successful_years] 
    + [
        f"Change_{prev_str}_{last_str}",
        f"Pct_Change_{prev_str}_{last_str}",
        f"Cum_Change_{first_str}_{last_str}",
        f"Cum_Pct_Change_{first_str}_{last_str}",
    ]
)
pivot = pivot[cols]


# --------------------------------------------------------
# 5. Add metadata and save JSON
# --------------------------------------------------------
records = pivot.to_dict(orient="records")

metadata = {
    "metadata": {
        "source": "U.S. Census Bureau, ACS 5-Year Detailed Table B25001 (Total Housing Units)",
        "units": DATA_UNITS,
        "islandwide_aggregation": "Sum of all 78 Municipios",
        "data_years": [str(y) for y in successful_years],
        "updated": datetime.now().strftime("%Y-%m-%d"),
        "notes": (
            "Nominal values represent the total estimate of Housing Units. "
            "Percentage changes are calculated based on these counts."
        )
    }
}

records.append(metadata)

json_path = OUT / f"municipios_acs_housing_{START_YEAR}_{last_str}_wide.json"
# The output filename will be adjusted based on the last successful year, e.g., municipios_acs_housing_2013_2023_wide.json
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(records, f, indent=2, ensure_ascii=False)

print(f"\n✅ Saved JSON → {json_path.name}")
print("🎉 Structural parity with dashboard template achieved.")
