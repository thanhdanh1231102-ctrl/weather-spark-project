import os
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import arrays_zip, col, explode, lit, to_date


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DATA_DIR = PROJECT_ROOT / "data" / "processed"


def create_spark_session() -> SparkSession:
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    return (
        SparkSession.builder.appName("TransformWeatherData")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def latest_raw_file() -> Path:
    raw_files = sorted(RAW_DATA_DIR.glob("weather_*.json"))

    if not raw_files:
        raise FileNotFoundError(f"No raw weather files found in {RAW_DATA_DIR}")

    return raw_files[-1]


def write_dataset(df, dataset_name: str, ingestion_date: str) -> None:
    output_path = PROCESSED_DATA_DIR / dataset_name / f"ingestion_date={ingestion_date}"

    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(str(output_path))
    )


def transform_weather(raw_file: Path) -> None:
    ingestion_date = date.today().isoformat().replace("-", "_")
    spark = create_spark_session()

    try:
        raw_df = spark.read.option("multiLine", "true").json(str(raw_file))

        metadata_df = raw_df.select(
            col("latitude"),
            col("longitude"),
            col("generationtime_ms"),
            col("utc_offset_seconds"),
            col("timezone"),
            col("timezone_abbreviation"),
            col("elevation"),
            lit(raw_file.name).alias("source_file"),
            lit(ingestion_date).alias("ingestion_date"),
        )

        hourly_df = (
            raw_df.select(
                explode(
                    arrays_zip(
                        col("hourly.time"),
                        col("hourly.temperature_2m"),
                        col("hourly.relative_humidity_2m"),
                        col("hourly.precipitation"),
                        col("hourly.wind_speed_10m"),
                    )
                ).alias("hourly_data")
            )
            .select(
                col("hourly_data.time").alias("time"),
                to_date(col("hourly_data.time")).alias("weather_date"),
                col("hourly_data.temperature_2m").alias("temperature_2m"),
                col("hourly_data.relative_humidity_2m").alias("relative_humidity_2m"),
                col("hourly_data.precipitation").alias("precipitation"),
                col("hourly_data.wind_speed_10m").alias("wind_speed_10m"),
                lit(raw_file.name).alias("source_file"),
                lit(ingestion_date).alias("ingestion_date"),
            )
        )

        daily_df = (
            raw_df.select(
                explode(
                    arrays_zip(
                        col("daily.time"),
                        col("daily.temperature_2m_max"),
                        col("daily.temperature_2m_min"),
                        col("daily.precipitation_sum"),
                    )
                ).alias("daily_data")
            )
            .select(
                to_date(col("daily_data.time")).alias("weather_date"),
                col("daily_data.temperature_2m_max").alias("temperature_2m_max"),
                col("daily_data.temperature_2m_min").alias("temperature_2m_min"),
                col("daily_data.precipitation_sum").alias("precipitation_sum"),
                lit(raw_file.name).alias("source_file"),
                lit(ingestion_date).alias("ingestion_date"),
            )
        )

        write_dataset(metadata_df, "metadata", ingestion_date)
        write_dataset(hourly_df, "hourly", ingestion_date)
        write_dataset(daily_df, "daily", ingestion_date)

        print(f"Transformed {raw_file}")
        print(f"Saved metadata, hourly, and daily datasets to {PROCESSED_DATA_DIR}")
    finally:
        spark.stop()


def main() -> None:
    transform_weather(latest_raw_file())


if __name__ == "__main__":
    main()
