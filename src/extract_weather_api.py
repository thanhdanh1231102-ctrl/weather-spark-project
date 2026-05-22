import json
from datetime import date
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen


API_URL = "https://api.open-meteo.com/v1/forecast"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


def fetch_weather(latitude: float, longitude: float) -> dict:
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "forecast_days": 1,
        "timezone": "auto",
    }

    url = f"{API_URL}?{urlencode(params)}"

    with urlopen(url, timeout=30) as response:
        if response.status != 200:
            raise RuntimeError(f"Open-Meteo request failed with status {response.status}")

        return json.loads(response.read().decode("utf-8"))


def save_raw_weather(data: dict) -> Path:
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_path = RAW_DATA_DIR / f"weather_{date.today().isoformat().replace('-', '_')}.json"

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)

    return output_path


def main() -> None:
    # Ho Chi Minh City coordinates. Change these for another city.
    weather_data = fetch_weather(latitude=10.8231, longitude=106.6297)
    output_path = save_raw_weather(weather_data)

    print(f"Saved raw weather data to {output_path}")


if __name__ == "__main__":
    main()

