import json
from datetime import datetime
from pathlib import Path

# Automatically find the JSON file in the same folder as this script
path = Path(__file__).resolve().parent / "municipios_cbp_total_employment_2010_2023_wide.json"

# Load current file
with open(path, "r", encoding="utf-8") as f:
    data = json.load(f)

# Ensure metadata block isn’t already there
if not isinstance(data[-1], dict) or "metadata" not in data[-1]:
    metadata = {
        "metadata": {
            "source": "U.S. Census Bureau, County Business Patterns (CBP), NAICS 00 (All Industries)",
            "units": "Number of Paid Employees (as of March 12)",
            "islandwide_aggregation": True,
            "data_years": [str(y) for y in range(2012, 2024)],
            "updated": datetime.now().strftime("%Y-%m-%d"),
            "notes": (
                "Nominal employment values represent total paid employees by municipio. "
                "All RealIncome_* and Real_* fields are null placeholders for full "
                "compatibility with municipios_acs_s1901_median_income_2010_2023_wide.json."
            )
        }
    }

    # Append metadata and save
    data.append(metadata)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"✅ Metadata successfully appended to: {path.name}")
else:
    print("ℹ️ Metadata block already exists, no changes made.")
