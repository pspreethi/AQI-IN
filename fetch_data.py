import pandas as pd
import requests
import datetime
import os
from dotenv import load_dotenv
import logging
from pathlib import Path


# Setup Logging
logging.basicConfig(
    filename="fetch_data.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

env_path = Path("config_template.env")
load_dotenv(dotenv_path=env_path)

# Load API key from .env
API_KEY = os.getenv("OPENAQ_API_KEY")

if not API_KEY:
    logging.error("Missing OPENAQ_API_KEY in .env file.")
    raise ValueError("OPENAQ_API_KEY is not set. Please add it to your .env file.")

HEADERS = {"X-API-Key": API_KEY}
BASE_LOCATION_URL = "https://api.openaq.org/v3/locations"
BASE_SENSOR_URL = "https://api.openaq.org/v3/sensors"


## Helper functions

def fetch_paginated_data(url, params=None):
    page = 1
    limit = 1000
    all_results = []
    while True:
        full_url = f"{url}?page={page}&limit={limit}"
        response = requests.get(full_url, headers=HEADERS, params=params)

        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])
            if not results:
                break
            all_results.extend(results)
            page += 1
        else:
            logging.error(f"Failed to fetch {url}: {response.status_code}")
            break
    return all_results

def normalize_sensor_data(sensor_ids):
    all_sensor_data = []
    params = {
        "datetime_from": "2020-01-01",
        "datetime_to": "2025-02-25",
        "limit": 1000
    }

    for s_id in sensor_ids:
        logging.info(f"Fetching sensor {s_id}")
        page = 1
        while True:
            params["page"] = page
            url = f"{BASE_SENSOR_URL}/{s_id}/measurements/daily"
            response = requests.get(url, headers=HEADERS, params=params)

            if response.status_code == 200:
                data = response.json()
                if not data.get("results"):
                    break
                for record in data["results"]:
                    record["sensor_id"] = s_id
                    all_sensor_data.append(record)
                page += 1
            else:
                logging.error(f"Sensor {s_id} failed on page {page}")
                break

    return pd.json_normalize(all_sensor_data)


def main():
    # Step 1: Fetch all location metadata
    all_locations = fetch_paginated_data(BASE_LOCATION_URL)

    # Step 2: Filter only Indian, stationary, licensed PM2.5 sensors
    in_locations = [loc for loc in all_locations if loc.get("country", {}).get("code") == "IN"]

    df_total = pd.json_normalize(
        all_locations,
        record_path=["sensors"],
        meta=[
            "id", "name", "locality", "timezone", "isMobile", "isMonitor", "licenses", "instruments",
            ["bounds"], "distance",["datetimeFirst", "utc"], ["datetimeLast", "utc"],
            ["country", "id"], ["country", "code"], ["country", "name"],
            ["owner", "id"], ["owner", "name"],
            ["provider", "id"], ["provider", "name"],
            ["coordinates", "latitude"], ["coordinates", "longitude"]
        ],
        record_prefix="s_",
        errors="ignore"
    )

    df_all = pd.json_normalize(
        in_locations,
        record_path=["sensors"],
        meta=[
            "id", "name", "locality", "timezone", "isMobile", "isMonitor", "licenses", "instruments",
            ["bounds"], "distance",["datetimeFirst", "utc"], ["datetimeLast", "utc"],
            ["country", "id"], ["country", "code"], ["country", "name"],
            ["owner", "id"], ["owner", "name"],
            ["provider", "id"], ["provider", "name"],
            ["coordinates", "latitude"], ["coordinates", "longitude"]
        ],
        record_prefix="s_",
        errors="ignore"
    )

    df_pm25 = df_all[df_all["s_name"] == "pm25 µg/m³"]

    # Step 3: Convert date columns to datetime
    df_pm25["datetimeFirst.utc"] = pd.to_datetime(df_pm25["datetimeFirst.utc"])
    df_pm25["datetimeLast.utc"] = pd.to_datetime(df_pm25["datetimeLast.utc"])
    current_year = datetime.datetime.now().year
    df_pm25["data_duration_years"] = (df_pm25["datetimeLast.utc"] - df_pm25["datetimeFirst.utc"]).dt.days / 365

    # Step 4: Filter sensors
    df_filtered = df_pm25[
        (df_pm25["data_duration_years"] >= 2) &
        (df_pm25["datetimeLast.utc"].dt.year == current_year) &
        (df_pm25["provider.name"] == "AirNow") &
        (df_pm25["locality"].notna()) &
        (df_pm25["licenses"].notna())
    ]

    # Step 5: Get list of sensor IDs and fetch their data
    sensor_ids = list(df_filtered["s_id"])
    df_filtered.to_csv("data/locations.csv")
    logging.info(f"Sensor Id's:  {sensor_ids}")
    df_sensor_data = normalize_sensor_data(sensor_ids)

    # Step 6: Save final output
    if not df_sensor_data.empty:
        output_file = f"data/openaq_combined_data.csv"
        os.makedirs("data", exist_ok=True)
        df_sensor_data.to_csv(output_file, index=False)
        logging.info(f"Saved data to {output_file}")
    else:
        logging.warning("No sensor data fetched.")

if __name__ == "__main__":
    main()

