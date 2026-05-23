import csv
import os
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable

import mysql.connector
from mysql.connector import Error
from mysql.connector import MySQLConnection


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"
DATABASE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")


def load_env_file(env_path: Path = PROJECT_ROOT / ".env") -> None:
    if not env_path.exists():
        return

    with env_path.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def database_name() -> str:
    name = os.getenv("MYSQL_DATABASE", "weather_db").strip()

    if not DATABASE_NAME_PATTERN.fullmatch(name):
        raise ValueError(
            "MYSQL_DATABASE must contain only letters, numbers, and underscores"
        )

    return name


def mysql_config(include_database: bool = True) -> dict[str, Any]:
    load_env_file()

    config = {
        "user": os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", ""),
        "connection_timeout": 10,
    }

    unix_socket = os.getenv("MYSQL_UNIX_SOCKET")
    if unix_socket:
        config["unix_socket"] = unix_socket
    else:
        port = os.getenv("MYSQL_PORT", "3306")
        try:
            config["port"] = int(port)
        except ValueError as error:
            raise ValueError("MYSQL_PORT must be a number") from error

        config["host"] = os.getenv("MYSQL_HOST", "127.0.0.1")

    if include_database:
        config["database"] = database_name()

    return config


def connect(include_database: bool = True) -> MySQLConnection:
    try:
        return mysql.connector.connect(**mysql_config(include_database=include_database))
    except Error as error:
        target = database_name() if include_database else "MySQL server"
        config = mysql_config(include_database=include_database)
        user = config.get("user")
        host = config.get("host", config.get("unix_socket"))
        raise ConnectionError(
            f"Cannot connect to {target} as user '{user}' through '{host}': {error}. "
            "Check MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST/MYSQL_UNIX_SOCKET, and MySQL user permissions."
        ) from error


def create_database() -> None:
    database = database_name()

    with connect(include_database=False) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"CREATE DATABASE IF NOT EXISTS `{database}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )


def create_tables(connection: MySQLConnection) -> None:
    statements = [
        """
        CREATE TABLE IF NOT EXISTS locations (
            location_id INT PRIMARY KEY,
            city_name VARCHAR(100) NOT NULL,
            latitude DOUBLE,
            longitude DOUBLE,
            utc_offset_seconds INT,
            timezone VARCHAR(100),
            timezone_abbreviation VARCHAR(20),
            elevation DOUBLE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS hourly_weather (
            location_id INT NOT NULL,
            time DATETIME NOT NULL,
            weather_date DATE,
            temperature_2m DOUBLE,
            relative_humidity_2m DOUBLE,
            precipitation DOUBLE,
            wind_speed_10m DOUBLE,
            PRIMARY KEY (location_id, time)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS daily_weather (
            location_id INT NOT NULL,
            weather_date DATE NOT NULL,
            temperature_2m_max DOUBLE,
            temperature_2m_min DOUBLE,
            precipitation_sum DOUBLE,
            PRIMARY KEY (location_id, weather_date)
        )
        """,
    ]

    with connection.cursor() as cursor:
        for statement in statements:
            cursor.execute(statement)
    connection.commit()


def latest_ingestion_dir(dataset_name: str) -> Path:
    dataset_dir = PROCESSED_DATA_DIR / dataset_name
    ingestion_dirs = sorted(path for path in dataset_dir.glob("ingestion_date=*") if path.is_dir())

    if not ingestion_dirs:
        raise FileNotFoundError(f"No ingestion folders found in {dataset_dir}")

    return ingestion_dirs[-1]


def csv_file_in(directory: Path) -> Path:
    csv_files = sorted(directory.glob("part-*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No Spark CSV part file found in {directory}")

    return csv_files[0]


def read_csv_rows(csv_path: Path, converters: dict[str, Callable[[str], Any]]) -> list[dict[str, Any]]:
    with csv_path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        rows = []

        for row in reader:
            converted_row = {}
            for key, value in row.items():
                if value == "":
                    converted_row[key] = None
                else:
                    converted_row[key] = converters.get(key, str)(value)
            rows.append(converted_row)

    return rows


def parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value)


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def load_locations(connection: MySQLConnection) -> int:
    csv_path = csv_file_in(PROCESSED_DATA_DIR / "locations")
    rows = read_csv_rows(
        csv_path,
        {
            "location_id": int,
            "latitude": float,
            "longitude": float,
            "utc_offset_seconds": int,
            "elevation": float,
        },
    )

    statement = """
        INSERT INTO locations (
            location_id, city_name, latitude, longitude, utc_offset_seconds,
            timezone, timezone_abbreviation, elevation
        )
        VALUES (
            %(location_id)s, %(city_name)s, %(latitude)s, %(longitude)s,
            %(utc_offset_seconds)s, %(timezone)s, %(timezone_abbreviation)s,
            %(elevation)s
        )
        ON DUPLICATE KEY UPDATE
            city_name = VALUES(city_name),
            latitude = VALUES(latitude),
            longitude = VALUES(longitude),
            utc_offset_seconds = VALUES(utc_offset_seconds),
            timezone = VALUES(timezone),
            timezone_abbreviation = VALUES(timezone_abbreviation),
            elevation = VALUES(elevation)
    """

    with connection.cursor() as cursor:
        if rows:
            cursor.executemany(statement, rows)
    connection.commit()

    return len(rows)


def load_hourly_weather(connection: MySQLConnection) -> int:
    csv_path = csv_file_in(latest_ingestion_dir("hourly"))
    rows = read_csv_rows(
        csv_path,
        {
            "location_id": int,
            "time": parse_datetime,
            "weather_date": parse_date,
            "temperature_2m": float,
            "relative_humidity_2m": float,
            "precipitation": float,
            "wind_speed_10m": float,
        },
    )

    statement = """
        INSERT INTO hourly_weather (
            location_id, time, weather_date, temperature_2m,
            relative_humidity_2m, precipitation, wind_speed_10m
        )
        VALUES (
            %(location_id)s, %(time)s, %(weather_date)s, %(temperature_2m)s,
            %(relative_humidity_2m)s, %(precipitation)s, %(wind_speed_10m)s
        )
        ON DUPLICATE KEY UPDATE
            weather_date = VALUES(weather_date),
            temperature_2m = VALUES(temperature_2m),
            relative_humidity_2m = VALUES(relative_humidity_2m),
            precipitation = VALUES(precipitation),
            wind_speed_10m = VALUES(wind_speed_10m)
    """

    with connection.cursor() as cursor:
        if rows:
            cursor.executemany(statement, rows)
    connection.commit()

    return len(rows)


def load_daily_weather(connection: MySQLConnection) -> int:
    csv_path = csv_file_in(latest_ingestion_dir("daily"))
    rows = read_csv_rows(
        csv_path,
        {
            "location_id": int,
            "weather_date": parse_date,
            "temperature_2m_max": float,
            "temperature_2m_min": float,
            "precipitation_sum": float,
        },
    )

    statement = """
        INSERT INTO daily_weather (
            location_id, weather_date, temperature_2m_max,
            temperature_2m_min, precipitation_sum
        )
        VALUES (
            %(location_id)s, %(weather_date)s, %(temperature_2m_max)s,
            %(temperature_2m_min)s, %(precipitation_sum)s
        )
        ON DUPLICATE KEY UPDATE
            temperature_2m_max = VALUES(temperature_2m_max),
            temperature_2m_min = VALUES(temperature_2m_min),
            precipitation_sum = VALUES(precipitation_sum)
    """

    with connection.cursor() as cursor:
        if rows:
            cursor.executemany(statement, rows)
    connection.commit()

    return len(rows)


def main() -> None:
    create_database()

    with connect() as connection:
        create_tables(connection)
        locations_count = load_locations(connection)
        hourly_count = load_hourly_weather(connection)
        daily_count = load_daily_weather(connection)

    print(f"Loaded {locations_count} locations")
    print(f"Loaded {hourly_count} hourly weather rows")
    print(f"Loaded {daily_count} daily weather rows")


if __name__ == "__main__":
    main()
