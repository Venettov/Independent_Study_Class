import requests

API_KEY = "29dc42832697b740f9eff8ae8d61b9e544478c2b"
year = 2010
vars_short = "NAME,S2301_C04_001E,S2301_C03_001E"
url = (
    f"https://api.census.gov/data/{year}/acs/acs5/subject"
    f"?get={vars_short}&for=county:*&in=state:72&key={API_KEY}"
)
r = requests.get(url)
print(r.status_code, r.text)
