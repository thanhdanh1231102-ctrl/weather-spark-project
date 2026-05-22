# Weather Spark Project

Project nhỏ dùng để thực hành quy trình ETL với Python và PySpark:

1. Lấy dữ liệu thời tiết từ Open-Meteo API.
2. Lưu dữ liệu thô vào thư mục `data/raw`.
3. Dùng PySpark để biến đổi dữ liệu.
4. Xuất dữ liệu đã xử lý ra `data/processed`.

Mặc định project đang lấy dữ liệu thời tiết của Thành phố Hồ Chí Minh.

## Công nghệ sử dụng

- Python
- PySpark
- Open-Meteo API
- Jupyter Notebook

## Cấu trúc project

```text
weather-spark-project/
├── README.md
├── requirements.txt
├── notebooks/
│   └── analysis.ipynb
├── src/
│   ├── extract_weather_api.py
│   ├── transform_weather_spark.py
│   └── analyze_weather.py
└── data/
    ├── raw/
    └── processed/
```

Trong đó:

- `src/extract_weather_api.py`: gọi Open-Meteo API và lưu dữ liệu JSON thô.
- `src/transform_weather_spark.py`: đọc file JSON mới nhất và biến đổi bằng PySpark.
- `src/analyze_weather.py`: file dành cho bước phân tích dữ liệu sau khi transform.
- `notebooks/analysis.ipynb`: notebook dùng để khám phá và phân tích dữ liệu.
- `data/raw`: chứa dữ liệu thô lấy từ API.
- `data/processed`: chứa dữ liệu đã xử lý.

Thư mục `data/` không được đưa lên GitHub vì đây là dữ liệu sinh ra trong quá trình chạy project.

## Cài đặt

Clone project về máy:

```bash
git clone https://github.com/thanhdanh1231102-ctrl/weather-spark-project.git
cd weather-spark-project
```

Tạo môi trường ảo:

```bash
python -m venv .venv
source .venv/bin/activate
```

Cài thư viện:

```bash
pip install -r requirements.txt
```

Lưu ý: PySpark cần Java. Nếu máy chưa có Java, hãy cài JDK trước khi chạy script transform.

## Cách chạy project

### 1. Lấy dữ liệu thời tiết từ API

```bash
python src/extract_weather_api.py
```

Sau khi chạy, project sẽ tạo file JSON trong:

```text
data/raw/
```

Ví dụ:

```text
data/raw/weather_2026_05_22.json
```

### 2. Transform dữ liệu bằng PySpark

```bash
python src/transform_weather_spark.py
```

Script này sẽ đọc file `weather_*.json` mới nhất trong `data/raw` và tạo các dataset sau:

```text
data/processed/metadata/
data/processed/hourly/
data/processed/daily/
```

Các dataset được ghi theo ngày xử lý, ví dụ:

```text
data/processed/hourly/ingestion_date=2026_05_22/
```

## Dữ liệu đầu ra

### Metadata

Chứa thông tin chung của request:

- latitude
- longitude
- timezone
- elevation
- source_file
- ingestion_date

### Hourly

Chứa dữ liệu thời tiết theo giờ:

- time
- weather_date
- temperature_2m
- relative_humidity_2m
- precipitation
- wind_speed_10m
- source_file
- ingestion_date

### Daily

Chứa dữ liệu thời tiết theo ngày:

- weather_date
- temperature_2m_max
- temperature_2m_min
- precipitation_sum
- source_file
- ingestion_date

## Thay đổi thành phố

Mặc định file `src/extract_weather_api.py` đang dùng tọa độ Thành phố Hồ Chí Minh:

```python
weather_data = fetch_weather(latitude=10.8231, longitude=106.6297)
```

Muốn lấy dữ liệu cho thành phố khác, thay `latitude` và `longitude` bằng tọa độ mới.

## Ghi chú về Git

Project đã ignore dữ liệu trong `.gitignore`:

```gitignore
data/
*.csv
*.json
*.parquet
```

Vì vậy khi commit lên GitHub, chỉ source code và tài liệu được đưa lên. Dữ liệu trong `data/` vẫn nằm trên máy local nhưng không được upload.

Quy trình update project lên GitHub:

```bash
git status
git add .
git commit -m "Mo ta thay doi"
git push
```
