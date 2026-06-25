"""
Downloads reference data files used by the pipeline.

Run once during initial setup:
    python scripts/download_static_data.py
"""

from pathlib import Path

import pandas as pd
import requests

DATA_DIRECTORY = Path("src/gdelt")


# --- countries.geojson - saved to src/gdelt ---

GEOJSON_URL = (
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_110m_admin_0_countries.geojson"
)
geojson_path = DATA_DIRECTORY / "countries.geojson"
geojson_path.write_bytes(requests.get(GEOJSON_URL, timeout=60).content)
print(f"[SUCCESS] Downloaded {geojson_path}")


# --- fips_to_iso.csv (FIPS 10-4 to ISO 3166 mapping) - saved to src/gdelt ---

FIPS_URL = (
    "https://raw.githubusercontent.com/mysociety/gazemaster/data/fips-10-4-to-iso-country-codes.csv"
)
fips_dataframe = pd.read_csv(FIPS_URL)
fips_dataframe.columns = ["fips", "iso2", "name"]
fips_path = DATA_DIRECTORY / "fips_to_iso.csv"
fips_dataframe.to_csv(fips_path, index=False)
print(f"[SUCCESS] Downloaded {fips_path}")
