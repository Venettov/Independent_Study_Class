import requests
import json
import time
import datetime
import os

def fetch_usgs_earthquakes_yearly(start_year=2010, end_year=None,
                                  minlatitude=17.5, maxlatitude=19.5,
                                  minlongitude=-67.5, maxlongitude=-65.0,
                                  minmagnitude=0):
    """
    Fetches earthquake data for Puerto Rico from USGS API year by year
    and saves results as a single JSON file in the same directory as this script.
    """

    if end_year is None:
        end_year = datetime.datetime.now().year

    url = 'https://earthquake.usgs.gov/fdsnws/event/1/query'
    all_quakes = []

    for year in range(start_year, end_year + 1):
        starttime = f"{year}-01-01"
        endtime = f"{year}-12-31"
        params = {
            'format': 'geojson',
            'starttime': starttime,
            'endtime': endtime,
            'minlatitude': minlatitude,
            'maxlatitude': maxlatitude,
            'minlongitude': minlongitude,
            'maxlongitude': maxlongitude,
            'minmagnitude': minmagnitude,
            'orderby': 'time-asc',
            'limit': 20000  # USGS default cap is 20k per request
        }

        print(f"Fetching {year} ...")
        try:
            resp = requests.get(url, params=params, timeout=60)
            if resp.status_code != 200:
                print(f"⚠️  {year} failed: {resp.status_code} {resp.text[:200]}")
                continue

            data = resp.json()
            for f in data.get('features', []):
                p = f.get('properties', {})
                g = f.get('geometry', {})
                c = g.get('coordinates', [None, None, None])

                if not c or len(c) < 3:
                    continue

                all_quakes.append({
                    'date': time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(p.get('time')/1000)),
                    'place': p.get('place'),
                    'mag': p.get('mag'),
                    'latitude': c[1],
                    'longitude': c[0],
                    'depth_km': c[2],
                    'url': p.get('url')
                })

            print(f"✅ {year}: {len(data.get('features', []))} records")
            time.sleep(1)  # small delay between requests to avoid hitting rate limits

        except Exception as e:
            print(f"❌ Error fetching {year}: {e}")
            continue

    # Save JSON in the same folder as this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, 'puerto_rico_earthquakes.json')

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_quakes, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Saved {len(all_quakes)} total earthquake records to {output_path}")

if __name__ == "__main__":
    fetch_usgs_earthquakes_yearly()
