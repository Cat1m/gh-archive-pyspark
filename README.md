# GH Archive PySpark ETL

Dự án học PySpark bằng cách xử lý dữ liệu thật: [GH Archive](https://www.gharchive.org)
— toàn bộ sự kiện public của GitHub, JSON lồng nhau, mỗi giờ 1 file.

Kiến trúc medallion (Bronze → Silver → Gold), chạy PySpark **local** (không
Databricks) — trọng tâm là hiểu engine (lazy evaluation, transformation vs
action, shuffle, partition), không phải hạ tầng.

## Trạng thái

- [x] Step 0 — Setup môi trường + SparkSession
- [x] Step 1 — Ingestion GH Archive
- [x] Step 2 — Khám phá nested schema (lazy evaluation)
- [ ] Step 3 — Bronze (partition, transformation vs action)
- [ ] Step 4 — Silver (flatten + explode)
- [ ] Step 5 — Gold (aggregation + shuffle)
- [ ] Step 6 — Tối ưu + hiểu engine (cache, partition, skew)
- [ ] Step 7 — Portfolio polish

## Cách chạy (Step 0 + 1)

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt

# Kiểm tra SparkSession khởi tạo được, xem Spark UI
python spark/session.py

# Tải 1 ngày dữ liệu GH Archive (24 file .json.gz) về data/raw/
python ingestion/download_gharchive.py --start 2024-01-01 --end 2024-01-01
```

## Future work

- Đưa lên cluster thật (Databricks Free Edition / EMR) để chạy quy mô năm.
- Streaming: đọc GH Archive gần real-time bằng Spark Structured Streaming.
- Thêm orchestration (Airflow/Dagster) để tự tải + chạy mỗi giờ.
- Delta/Iceberg thay Parquet thô để có ACID + time travel.
