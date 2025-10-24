import json

INPUT_FILE = "municipios_cbp_total_employment_2010_2023_wide_with_PR.json"
OUTPUT_FILE = "municipios_cbp_fixed_for_web.json"

# Years known to have missing/zero data that should be null
YEARS_TO_CHECK = ['2010', '2011', '2012']
BASE_YEAR = 2012

def fix_employment_data(data):
    # Find the row for Puerto Rico aggregate and other municipalities that need cleaning
    for row in data:
        municipio = row['Municipio']
        
        # --- 1. Fix Puerto Rico Aggregate Row ---
        if municipio == "Puerto Rico":
            # Fix the non-numeric '0.0' or 'null' data points for 2010 and 2011
            for year in ['2010', '2011']:
                row[year] = None  # Use None for null in JSON
            
            # Recalculate or fix the cumulative percentage change (optional but good practice)
            val_2023 = row['2023']
            val_2012 = row[str(BASE_YEAR)]
            if val_2012 and val_2012 != 0:
                # Set the cumulative percentage change field to the value based on 2012
                # Note: The JavaScript itself recalculates this, but we'll clean the source anyway.
                row['Cum_Pct_Change_2010_2023'] = ((val_2023 - val_2012) / val_2012) * 100
            else:
                row['Cum_Pct_Change_2010_2023'] = None

        # --- 2. Standardize Missing/Zero Data for All Municipios ---
        # Convert explicit 0s in the base year (2012) to None (null) for clean plotting
        if row.get(str(BASE_YEAR)) == 0:
            row[str(BASE_YEAR)] = None

        # Also ensure 2010/2011 are None for all rows if they exist as 0
        if row.get('2010') == 0:
             row['2010'] = None
        if row.get('2011') == 0:
             row['2011'] = None

    # Remove the final metadata row which has the entire JSON structure as its Municipio name
    if data and data[-1]['Municipio'].startswith("{'source'"):
        data.pop()
        
    return data

# --- Execution ---
try:
    with open(INPUT_FILE, 'r') as f:
        data = json.load(f)
    
    fixed_data = fix_employment_data(data)
    
    with open(OUTPUT_FILE, 'w') as f:
        # Use json.dump with ensure_ascii=False for clean string output
        json.dump(fixed_data, f, indent=2, ensure_ascii=False)
        
    print(f"✅ Success! Data fixed and saved to {OUTPUT_FILE}")

except FileNotFoundError:
    print(f"🚨 Error: Input file '{INPUT_FILE}' not found. Please check your path.")
except Exception as e:
    print(f"🚨 An error occurred during processing: {e}")
