"""Bronze layer — đọc toàn bộ data/raw/*.json.gz, thêm cột event_date, ghi
Parquet partition theo ngày.

Bronze KHÔNG làm sạch/biến đổi nội dung — giữ nguyên union schema thô của
GH Archive (đã khảo sát ở docs/schema_notes.md, docs/data_profiling.md).
Việc duy nhất Bronze thêm vào là 1 cột kỹ thuật (event_date) để phục vụ
partition — đây là ranh giới rõ ràng giữa Bronze (raw, chỉ thêm metadata)
và Silver (Step 4: flatten, explode, lọc rác).

Usage:
    python spark/bronze.py
"""

import sys
from pathlib import Path

# Tránh crash khi in tiếng Việt có dấu trên console Windows dùng codepage
# cp1252 mặc định (cùng pattern với spark/session.py, ingestion/download_gharchive.py).
sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Import này có side-effect quan trọng: spark/session.py, lúc được import,
# tự động set HADOOP_HOME + PATH trỏ tới tools/hadoop/bin/ (winutils.exe,
# hadoop.dll) nếu đang chạy Windows — thiếu bước này .write.parquet() bên
# dưới sẽ crash (xem comment chi tiết trong spark/session.py).
import spark.session  # noqa: F401,E402
from pyspark.sql import SparkSession, functions as F  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
BRONZE_DATA_DIR = PROJECT_ROOT / "data" / "bronze"

# GH Archive created_at là string ISO-8601 dạng "2026-08-01T10:00:00Z" —
# khai báo format tường minh thay vì để Spark tự đoán, để chắc chắn parse
# đúng bất kể phiên bản Spark suy luận format mặc định khác nhau thế nào.
CREATED_AT_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"


def get_bronze_spark_session() -> SparkSession:
    """SparkSession riêng cho Bronze, KHÔNG dùng spark/session.py mặc định.

    docs/data_profiling.md (mục 7) đã ghi lại: driver.memory=2g (mặc định
    của spark/session.py, thiết kế cho các script demo nhỏ ở Step 0-2) làm
    Py4J crash OOM khi xử lý DataFrame union-schema (hàng trăm cột lồng
    nhau) trên 4 triệu dòng. Bronze ghi ra TOÀN BỘ union schema đó (không
    chỉ vài cột như analysis/profile_raw.py), nên cần driver lớn hơn hẳn.
    """
    spark = (
        SparkSession.builder.appName("gh-archive-bronze")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "6g")
        .getOrCreate()
    )
    print(f"[spark] Spark UI: {spark.sparkContext.uiWebUrl}")
    return spark


def main() -> None:
    spark = get_bronze_spark_session()

    # Liệt kê file tường minh bằng Path.glob() (Python) rồi truyền LIST vào
    # spark.read.json(), thay vì truyền 1 string wildcard "*.json.gz".
    # Lý do (docs/data_profiling.md mục 7): trên Windows, wildcard buộc
    # Spark gọi Hadoop globStatus() -> listStatus(), và listStatus cần
    # winutils.exe (HADOOP_HOME) mà project không cài — crash
    # UnsatisfiedLinkError. Đọc từng file cụ thể (kể cả nhiều file, truyền
    # dạng list) không đụng tới listStatus nên không bị ảnh hưởng.
    raw_files = sorted(str(p) for p in RAW_DATA_DIR.glob("*.json.gz"))
    if not raw_files:
        raise FileNotFoundError(
            f"Không tìm thấy file .json.gz nào trong {RAW_DATA_DIR} — "
            "chạy ingestion/download_gharchive.py trước."
        )
    print(f"[bronze] Đọc {len(raw_files)} file từ {RAW_DATA_DIR}")

    # ------------------------------------------------------------------
    # spark.read.json(raw_files): TRANSFORMATION. Spark chưa đọc byte nào
    # của 24 file này ở dòng này — chỉ suy luận schema (cần "nhìn" một phần
    # dữ liệu để đoán kiểu, nhưng đây là chi phí lập kế hoạch, không phải
    # xử lý toàn bộ 490MB).
    # ------------------------------------------------------------------
    df = spark.read.json(raw_files)

    # ------------------------------------------------------------------
    # withColumn(...): TRANSFORMATION — chỉ thêm 1 bước vào query plan
    # ("sau khi đọc xong, cast created_at rồi lấy phần ngày"). Chưa có
    # dòng dữ liệu nào được tính ra ở đây.
    # to_date() với format tường minh: parse string ISO-8601 thành DATE,
    # dùng làm giá trị partition (mỗi ngày 1 thư mục con).
    # ------------------------------------------------------------------
    df_with_date = df.withColumn(
        "event_date", F.to_date(F.col("created_at"), CREATED_AT_FORMAT)
    )

    # ------------------------------------------------------------------
    # .write.parquet(...) là ACTION — đây là lúc Spark THỰC SỰ chạy toàn bộ
    # chuỗi phía trên: mở 24 file .json.gz, giải nén, parse JSON theo union
    # schema, tính cột event_date cho từng dòng, rồi ghi kết quả ra Parquet.
    # Mọi transformation ở trên (read.json, withColumn) chỉ là kế hoạch cho
    # đến khi action này chạy.
    #
    # partitionBy("event_date"): Spark ghi ra 1 THƯ MỤC CON riêng cho mỗi
    # giá trị event_date (vd. event_date=2026-08-01/), không phải 1 file
    # phẳng. Lợi ích: các bước đọc lại sau này (Silver, Gold) có thể lọc
    # theo ngày mà KHÔNG cần quét toàn bộ Bronze — Spark chỉ đọc đúng thư
    # mục con khớp điều kiện WHERE/filter trên event_date (partition
    # pruning). Với demo 1 ngày sẽ chỉ thấy 1 thư mục con, nhưng cơ chế này
    # mới thật sự có giá trị khi Bronze tích luỹ nhiều ngày/tháng.
    #
    # mode("overwrite"): ghi đè nếu chạy lại — Bronze không cần merge/upsert
    # phức tạp ở quy mô demo này, ghi đè lại toàn bộ là đủ và đơn giản nhất.
    # ------------------------------------------------------------------
    print(f"[bronze] Ghi Parquet ra {BRONZE_DATA_DIR}, partition theo event_date...")
    df_with_date.write.mode("overwrite").partitionBy("event_date").parquet(
        str(BRONZE_DATA_DIR)
    )

    # In cấu trúc thư mục partition Spark vừa tạo, để thấy trực quan
    # partitionBy() sinh ra thư mục con "event_date=YYYY-MM-DD/" thế nào.
    print("\n[bronze] Cấu trúc thư mục Bronze sau khi ghi:")
    for entry in sorted(BRONZE_DATA_DIR.iterdir()):
        if entry.is_dir():
            files_inside = list(entry.glob("*.parquet"))
            print(f"  {entry.name}/  ({len(files_inside)} file parquet)")
            for f in files_inside:
                size_mb = f.stat().st_size / (1024 * 1024)
                print(f"    {f.name}  ({size_mb:.1f} MB)")

    # Action phụ để xác nhận số dòng đã ghi khớp với data_profiling.md
    # (4,071,556 event) — đọc lại từ chính Bronze vừa ghi, không phải từ
    # data/raw, để verify round-trip JSON -> Parquet không làm mất dòng nào.
    written = spark.read.parquet(str(BRONZE_DATA_DIR))
    total = written.count()
    print(f"\n[bronze] Tổng số dòng đọc lại từ Bronze: {total:,}")

    spark.stop()


if __name__ == "__main__":
    main()
