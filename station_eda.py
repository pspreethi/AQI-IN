import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from pathlib import Path

sns.set(style="whitegrid")

STATION_FOLDER = "data/stations"
OUTPUT_FOLDER = "outputs/eda"

def generate_station_eda(station_path):
    station_name = Path(station_path).stem.replace("_", " ")
    df = pd.read_csv(station_path, parse_dates=["to_local_date"], index_col="to_local_date")

    # Add time components
    df["weekday"] = df.index.day_name()
    df["month"] = df.index.month
    df["year"] = df.index.year

    month_map = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
                 7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}
    df["month_name"] = df["month"].map(month_map)

    # Create output folder
    out_path = Path(OUTPUT_FOLDER) / station_name.replace(" ", "_")
    out_path.mkdir(parents=True, exist_ok=True)

    # === WEEKDAY BOXPLOT ===
    plt.figure(figsize=(10, 5))
    sns.boxplot(x="weekday", y="summary.avg", data=df,
                order=["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"])
    plt.title(f"AQI by Day of Week - {station_name}")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(out_path / "weekday_boxplot.png")
    plt.close()

    # === MONTHLY BOXPLOT ===
    plt.figure(figsize=(10, 5))
    sns.boxplot(x="month_name", y="summary.avg", data=df,
                order=["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                       "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
    plt.title(f"Monthly AQI Seasonality - {station_name}")
    plt.tight_layout()
    plt.savefig(out_path / "monthly_boxplot.png")
    plt.close()

    # === YEARLY BOXPLOT ===
    plt.figure(figsize=(10, 5))
    sns.boxplot(x="year", y="summary.avg", data=df)
    plt.title(f"Year-over-Year AQI - {station_name}")
    plt.tight_layout()
    plt.savefig(out_path / "yearly_boxplot.png")
    plt.close()

    # === WEEKLY AVG LINE PLOT ===
    weekly = df["summary.avg"].resample("W").mean()
    plt.figure(figsize=(12, 5))
    plt.plot(weekly.index, weekly, marker='o')
    plt.title(f"Weekly Average AQI - {station_name}")
    plt.tight_layout()
    plt.savefig(out_path / "weekly_avg_line.png")
    plt.close()

    # === MONTHLY AVG LINE PLOT ===
    monthly = df["summary.avg"].resample("M").mean()
    plt.figure(figsize=(12, 5))
    plt.plot(monthly.index, monthly, marker='o')
    plt.title(f"Monthly Average AQI - {station_name}")
    plt.tight_layout()
    plt.savefig(out_path / "monthly_avg_line.png")
    plt.close()

    # === 90-DAY ROLLING TREND ===
    df["rolling_90"] = df["summary.avg"].rolling(window=90).mean()
    plt.figure(figsize=(12, 5))
    plt.plot(df.index, df["summary.avg"], alpha=0.4, label="Daily AQI")
    plt.plot(df.index, df["rolling_90"], color="red", label="90-Day MA")
    plt.title(f"Long-Term AQI Trend (90-Day MA) - {station_name}")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path / "rolling_90day_trend.png")
    plt.close()

    print(f"Plots saved for {station_name}")

# === Loop through all station files ===
if __name__ == "__main__":
    station_files = list(Path(STATION_FOLDER).glob("*.csv"))

    for file_path in station_files:
        generate_station_eda(file_path)

    print(" EDA complete for all stations!")
