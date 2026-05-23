# Weather Spark Project

A small project for practicing an ETL workflow with Python and PySpark:

1. Extract weather data from the Open-Meteo API.
2. Save raw data into `data/raw`.
3. Transform the raw data with PySpark.
4. Write processed datasets into `data/processed`.
5. Load the processed datasets into MySQL.

By default, this project collects weather data for Ho Chi Minh City, Vietnam.

## Tech Stack

- Python
- PySpark
- Open-Meteo API
- MySQL
- Jupyter Notebook

## Project Structure

```text
weather-spark-project/
├── README.md
├── requirements.txt
├── notebooks/
│   └── analysis.ipynb
├── src/
│   ├── extract_weather_api.py
│   ├── transform_weather_spark.py
│   ├── load_weather_to_mysql.py
│   └── analyze_weather.py
└── data/
    ├── raw/
    └── processed/
```

File and folder purpose:

- `src/extract_weather_api.py`: calls the Open-Meteo API and saves raw JSON data.
- `src/transform_weather_spark.py`: reads the latest raw JSON file and transforms it with PySpark.
- `src/load_weather_to_mysql.py`: creates MySQL tables and loads the processed CSV datasets.
- `src/analyze_weather.py`: reserved for analysis logic after the transform step.
- `notebooks/analysis.ipynb`: notebook for data exploration and analysis.
- `data/raw`: stores raw data extracted from the API.
- `data/processed`: stores transformed output datasets.

The `data/` directory is not committed to GitHub because it contains generated local data.

## Installation

Clone the repository:

```bash
git clone https://github.com/thanhdanh1231102-ctrl/weather-spark-project.git
cd weather-spark-project
```

Create a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Note: PySpark requires Java. Install a JDK before running the transform script if Java is not already available on your machine.

The MySQL load step requires a running MySQL server. The script uses these environment variables, with defaults if they are not set:

```bash
cp .env.example .env
```

Edit `.env` with your local MySQL credentials:

```text
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=your_password
MYSQL_DATABASE=weather_db
```

If your local MySQL server uses a Unix socket instead of host and port, set `MYSQL_UNIX_SOCKET`:

```text
MYSQL_UNIX_SOCKET=/tmp/mysql.sock
```

## How to Run

### 1. Extract weather data from the API

```bash
python src/extract_weather_api.py
```

After running the script, a JSON file will be created in:

```text
data/raw/
```

Example:

```text
data/raw/weather_2026_05_22.json
```

### 2. Transform data with PySpark

```bash
python src/transform_weather_spark.py
```

This script reads the latest `weather_*.json` file from `data/raw` and creates these datasets:

```text
data/processed/locations/
data/processed/hourly/
data/processed/daily/
```

The `hourly` and `daily` datasets are written by ingestion date, for example:

```text
data/processed/hourly/ingestion_date=2026_05_22/
```

### 3. Load processed data into MySQL

```bash
python src/load_weather_to_mysql.py
```

This script creates the database if it does not exist, creates the required tables, and loads the processed CSV files into MySQL.

The script loads:

- `data/processed/locations/`
- the latest `data/processed/hourly/ingestion_date=*/`
- the latest `data/processed/daily/ingestion_date=*/`

Rows are inserted with upsert logic, so running the script again updates existing records with the same primary key instead of creating duplicates.

## Output Data

### Locations

Location data. This dataset is overwritten when the transform job runs because location attributes are mostly static:

- location_id
- city_name
- latitude
- longitude
- utc_offset_seconds
- timezone
- timezone_abbreviation
- elevation

### Hourly

Hourly weather measurements:

- location_id
- time
- weather_date
- temperature_2m
- relative_humidity_2m
- precipitation
- wind_speed_10m


### Daily

Daily weather summary:

- location_id
- weather_date
- temperature_2m_max
- temperature_2m_min
- precipitation_sum

## MySQL Tables

The MySQL load script creates three tables:

- `locations`: one row per configured location.
- `hourly_weather`: hourly weather measurements, keyed by `location_id` and `time`.
- `daily_weather`: daily weather summaries, keyed by `location_id` and `weather_date`.

Default database name:

```text
weather_db
```


## Change the City

By default, `src/extract_weather_api.py` uses the coordinates of Ho Chi Minh City:

```python
weather_data = fetch_weather(latitude=10.8231, longitude=106.6297)
```

To collect weather data for another city, replace `latitude` and `longitude` with the new coordinates.

## Git Notes

Generated data is ignored in `.gitignore`:

```gitignore
data/
*.csv
*.json
*.parquet
```

This keeps source code and documentation on GitHub while leaving local data files on your machine.

Common workflow for updating the project on GitHub:

```bash
git status
git add .
git commit -m "Describe your changes"
git push
```
