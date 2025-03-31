import pandas as pd
from pathlib import Path
import os

input_file = "data/cleaned_openaq.csv"

def split_by_station(input_file, date_col="to_local_date", station_col="name", output_folder="data/stations"):
    """
    Splits a cleaned, interpolated dataset into one file per station (by 'name'),
    applies daily frequency, and interpolates missing values.
    """

    # === Load cleaned data ===
    df = pd.read_csv(input_file)

    # === Ensure datetime is parsed ===
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    # === Create output folder ===
    os.makedirs(output_folder, exist_ok=True)

    # === Loop through unique station names ===
    for station in df[station_col].dropna().unique():
        df_station = df[df[station_col] == station].copy()

        # Set datetime index
        df_station.set_index(date_col, inplace=True)

        # Step 1: Group by date (to remove duplicates)
        df_station = df_station.groupby(df_station.index).mean(numeric_only=True)

        # Step 2: Set daily frequency
        df_station = df_station.asfreq("D")

        # Step 3: Interpolate missing values
        aqi_cols = [
            'value', 'summary.min', 'summary.q02', 'summary.q25', 'summary.median',
            'summary.q75', 'summary.q98', 'summary.max', 'summary.avg', 'summary.sd'
        ]
        for col in aqi_cols:
            if col in df_station.columns:
                df_station[col] = df_station[col].interpolate(method="time")

        # Step 4: Save cleaned & interpolated file
        clean_name = station.replace(" ", "_").replace("/", "_")
        output_path = Path(output_folder) / f"{clean_name}.csv"
        df_station.to_csv(output_path)

        print(f" Saved: {output_path}")

    print(" All stations saved successfully!")

# === Run if used standalone ===
if __name__ == "__main__":
    split_by_station(input_file)
