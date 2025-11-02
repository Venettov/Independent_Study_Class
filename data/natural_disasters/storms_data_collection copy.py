import requests
import re
import json
from bs4 import BeautifulSoup  # Install with: pip install beautifulsoup4


# ------------------------------------------------------------
# 1. AUTO-DETECT THE CURRENT NOAA HURDAT2 DOWNLOAD LINK
# ------------------------------------------------------------
def get_hurdat2_link():
    """
    Scrape NOAA’s hurricane data page to find the current HURDAT2 Atlantic file.
    Returns a fully qualified URL to the latest .txt dataset.
    """
    archive_url = "https://www.nhc.noaa.gov/data/"
    print("🔍 Searching NOAA archive page for current HURDAT2 file...")
    r = requests.get(archive_url, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a['href']
        if "hurdat2" in href and href.endswith(".txt") and "atlantic" in href.lower():
            full_url = requests.compat.urljoin(archive_url, href)
            print(f"✅ Found HURDAT2 file: {full_url}\n")
            return full_url

    # fallback: generic search if NOAA removes 'atlantic' keyword
    for a in soup.find_all("a", href=True):
        href = a['href']
        if "hurdat2" in href and href.endswith(".txt"):
            full_url = requests.compat.urljoin(archive_url, href)
            print(f"✅ Found fallback HURDAT2 file: {full_url}\n")
            return full_url

    raise RuntimeError("❌ Could not locate HURDAT2 link on NOAA data page.")


# ------------------------------------------------------------
# 2. DOWNLOAD AND PARSE THE DATASET
# ------------------------------------------------------------
def download_hurdat2():
    """
    Download the NOAA HURDAT2 dataset using the dynamically found URL.
    """
    url = get_hurdat2_link()
    print(f"📥 Downloading HURDAT2 data from:\n   {url}")
    r = requests.get(url, timeout=90)
    r.raise_for_status()
    print("✅ Download successful.\n")
    return r.text


def parse_hurdat2(raw_text):
    """
    Parse the HURDAT2 text format into a structured list of storms.
    Each storm includes ID, name, and a list of track coordinates with wind data.
    """
    lines = raw_text.strip().splitlines()
    storms = []
    current_storm = None

    for line in lines:
        # Header lines (e.g., AL012017, MARIA, ...)
        if re.match(r"^[A-Z]{2,3}\d{6}", line):
            parts = [p.strip() for p in line.split(',')]
            storm_id = parts[0]
            name = parts[1].title()
            current_storm = {'id': storm_id, 'name': name, 'records': []}
            storms.append(current_storm)
        else:
            if current_storm:
                parts = [p.strip() for p in line.split(',')]
                try:
                    yyyymmdd = parts[0]
                    year = int(yyyymmdd[:4])
                    lat_str, lon_str = parts[4], parts[5]
                    lat = float(lat_str[:-1]) * (1 if lat_str.endswith('N') else -1)
                    lon = float(lon_str[:-1]) * (-1 if lon_str.endswith('W') else 1)
                    wind = int(parts[6])  # knots
                    status = parts[3]
                except Exception:
                    continue
                current_storm['records'].append({
                    'date': yyyymmdd,
                    'year': year,
                    'lat': lat,
                    'lon': lon,
                    'wind': wind,
                    'status': status
                })
    return storms


# ------------------------------------------------------------
# 3. FILTER FOR CARIBBEAN REGION AND TIME RANGE
# ------------------------------------------------------------
def filter_caribbean_tracks(storms, start_year=2015,
                            min_lat=8, max_lat=25,
                            min_lon=-90, max_lon=-55):
    """
    Filter storms that passed through the Caribbean between start_year and 2025.
    """
    caribbean_storms = []
    for storm in storms:
        path_in_caribbean = [
            r for r in storm['records']
            if start_year <= r['year'] <= 2025
            and min_lat <= r['lat'] <= max_lat
            and min_lon <= r['lon'] <= max_lon
        ]
        if path_in_caribbean:
            caribbean_storms.append({
                'name': storm['name'],
                'id': storm['id'],
                'path': path_in_caribbean
            })
    return caribbean_storms


# ------------------------------------------------------------
# 4. MAIN EXECUTION
# ------------------------------------------------------------
if __name__ == "__main__":
    try:
        # Step 1: Download latest NOAA data
        raw_text = download_hurdat2()

        # Step 2: Parse into structured format
        storms = parse_hurdat2(raw_text)

        # Step 3: Filter for Caribbean hurricanes (2015–2025)
        carib_storms = filter_caribbean_tracks(storms, start_year=2015)

        # Step 4: Save output JSON file
        output_path = "caribbean_hurricane_tracks_2015_2025.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(carib_storms, f, indent=2, ensure_ascii=False)

        print(f"✅ Saved {len(carib_storms)} Caribbean hurricane tracks (2015–2025)")
        print(f"💾 File created: {output_path}")

    except Exception as e:
        print(f"❌ Error: {e}")
