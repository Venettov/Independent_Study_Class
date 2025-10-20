#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
median_income_data_collection_auto_htmlready_metadata.py

Collects Median Household Income (nominal & real, CPI-adjusted to latest year)
for Puerto Rico municipios (2010–latest available) from ACS S1901 and FRED CPI.

Outputs:
- municipios_acs_s1901_median_income_2010_XXXX.csv
- municipios_acs_s1901_median_income_2010_XXXX_wide.json
  (HTML-ready for income.html, with metadata section)

Author: Andres Ruiz
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

API_KEY = "29dc42832697b740f9eff8ae8d61b9e544478c2b"  # Replace with your own key
OUT = Path(__file__).resolve().parent
CPI_FRED_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=CPIAUCSL"

# =============================
# HELPER FUNCTIONS
# =============================

def safe_float(val):
    try:
        return float(val)
    except Exception:
        return None

def pct_change(new, old):
    if new is None or old is None or old == 0:
        return None
    return ((new - old) / old) * 100

def year_available(year, api_key):
    """Check if ACS data exists for this year to prevent 404s."""
    test_url = f"https://api.census.gov/data/{year}/acs/acs5/subject/variables.json?key={api_key}"
    try:
        r = requests.get(test_url, timeout=10)
        return r.status_code == 200
    except Exception:
        return False

# =============================
# 1. DETERMINE AVAILABLE YEARS
# =============================

print("\n🧭 Checking available ACS 5-year data for Puerto Rico...")
current_year = date.today().year
years = [y for y in range(2010, current_year + 1) if year_available(y, API_KEY)]

if not years:
    raise RuntimeError("❌ No valid ACS years detected.")
print(f"✅ Available ACS years: {years}")

# =============================
# 2. DOWNLOAD ACS S1901 (Median Household Income)
# =============================

print("\n💰 Downloading Median Household Income (S1901)...")

VARS = ["NAME", "S1901_C01_012E"]
records = []

for i, year in enumerate(years, start=1):
    url = (
        f"https://api.census.gov/data/{year}/acs/acs5/subject"
        f"?get={','.join(VARS)}&for=county:*&in=state:72&key={API_KEY}"
    )
    sys.stdout.write(f"\rFetching {year} ({i}/{len(years)}) ...")
    sys.stdout.flush()
    try:
        r = requests.get(url, timeout=60)
        if r.status_code != 200:
            print(f"\n⚠️  Error {r.status_code} for {year}: {r.text[:150]}")
            continue
        data = r.json()
        header, *rows = data
        idx = {k: i for i, k in enumerate(header)}
        for row in rows:
            records.append({
                "year": year,
                "municipio": row[idx["NAME"]].replace(", Puerto Rico", ""),
                "income": safe_float(row[idx["S1901_C01_012E"]])
            })
    except Exception as e:
        print(f"\n❌ Failed for {year}: {e}")
        continue

print("\n✅ ACS income download complete.")
df = pd.DataFrame(records).dropna(subset=["income"])
df = df.sort_values(["municipio", "year"]).reset_index(drop=True)

# =============================
# 3. DOWNLOAD CPI (FRED)
# =============================

print("\n📈 Downloading CPI (CPIAUCSL) from FRED...")
cpi_df = pd.read_csv(CPI_FRED_URL)
cpi_df.columns = cpi_df.columns.str.lower()
cpi_df = cpi_df.rename(columns={"observation_date": "date", "cpiaucsl": "cpi"})
cpi_df["year"] = pd.to_datetime(cpi_df["date"]).dt.year
cpi_annual = cpi_df.groupby("year", as_index=False)["cpi"].mean()
cpi_annual = cpi_annual[cpi_annual["year"].between(min(years), max(years))].reset_index(drop=True)
print(f"CPI years available: {cpi_annual['year'].tolist()}")

cpi_dict = dict(zip(cpi_annual["year"], cpi_annual["cpi"]))
cpi_ref_year = max(years)
cpi_ref_val = cpi_dict.get(cpi_ref_year, max(cpi_dict.values()))

# =============================
# 4. ADJUST INCOME TO CONSTANT DOLLARS
# =============================

df["cpi"] = df["year"].map(cpi_dict)
df["real_income"] = df["income"] * (cpi_ref_val / df["cpi"])

# =============================
# 5. ADD ISLANDWIDE AVERAGE ("Puerto Rico")
# =============================

island = (
    df.groupby("year", as_index=False)["income"]
    .mean(numeric_only=True)
    .assign(municipio="Puerto Rico")
)
island["real_income"] = island["income"] * (cpi_ref_val / island["year"].map(cpi_dict))
df = pd.concat([df, island], ignore_index=True)
print("✅ Added islandwide average row for Puerto Rico.")

# =============================
# 6. SAVE LONG CSV
# =============================

csv_path = OUT / f"municipios_acs_s1901_median_income_2010_{cpi_ref_year}.csv"
df.to_csv(csv_path, index=False)
print(f"✅ Saved long format CSV → {csv_path.name}")

# =============================
# 7. BUILD WIDE JSON (HTML-compatible)
# =============================

pivot_nom = df.pivot(index="municipio", columns="year", values="income").reset_index()
pivot_real = df.pivot(index="municipio", columns="year", values="real_income").reset_index()
pivot_nom.columns.name = None
pivot_real.columns.name = None

# HTML-ready: numeric year keys for direct Chart.js/Leaflet use
pivot_nom = pivot_nom.rename(columns={y: str(y) for y in years})
pivot_real = pivot_real.rename(columns={y: f"RealIncome_{y}" for y in years})
wide = pivot_nom.merge(pivot_real, on="municipio", how="outer")

first, prev, last = years[0], years[-2], years[-1]

# Nominal change
wide[f"Change_{prev}_{last}"] = wide[str(last)] - wide[str(prev)]
wide[f"Pct_Change_{prev}_{last}"] = (wide[f"Change_{prev}_{last}"] / wide[str(prev)]) * 100
wide[f"Cum_Change_{first}_{last}"] = wide[str(last)] - wide[str(first)]
wide[f"Cum_Pct_Change_{first}_{last}"] = (wide[f"Cum_Change_{first}_{last}"] / wide[str(first)]) * 100

# Real (inflation-adjusted) change
wide[f"Real_Change_{prev}_{last}"] = wide[f"RealIncome_{last}"] - wide[f"RealIncome_{prev}"]
wide[f"Real_Pct_Change_{prev}_{last}"] = (wide[f"Real_Change_{prev}_{last}"] / wide[f"RealIncome_{prev}"]) * 100
wide[f"Real_Cum_Change_{first}_{last}"] = wide[f"RealIncome_{last}"] - wide[f"RealIncome_{first}"]
wide[f"Real_Cum_Pct_Change_{first}_{last}"] = (wide[f"Real_Cum_Change_{first}_{last}"] / wide[f"RealIncome_{first}"]) * 100

wide = wide.rename(columns={"municipio": "Municipio"})
records = wide.to_dict(orient="records")

# =============================
# 8. ADD METADATA
# =============================

metadata = {
    "metadata": {
        "source": "U.S. Census Bureau, ACS 5-Year Subject Tables (S1901)",
        "cpi_source": "U.S. Bureau of Labor Statistics, CPIAUCSL via FRED",
        "islandwide_average": True,
        "cpi_reference_year": cpi_ref_year,
        "data_years": years,
        "updated": datetime.now().strftime("%Y-%m-%d"),
        "notes": (
            "Nominal income values are as reported. RealIncome_* fields "
            "are adjusted to constant {0} USD using CPI-U (CPIAUCSL). "
            "This file is structured for direct compatibility with income.html "
            "on the Independent Study Class site."
        ).format(cpi_ref_year)
    }
}

# Append metadata as the final JSON entry
records.append(metadata)

# =============================
# 9. SAVE WIDE JSON
# =============================

json_path = OUT / f"municipios_acs_s1901_median_income_2010_{cpi_ref_year}_wide.json"
with open(json_path, "w", encoding="utf-8") as f:
    json.dump(records, f, ensure_ascii=False, indent=2)

print(f"✅ Saved HTML-compatible JSON with metadata → {json_path.name}")
print(f"\n🎉 All median income data (nominal, real, islandwide, documented) ready through {cpi_ref_year}.")
