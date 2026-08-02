"""Profile toàn bộ data/raw/*.json.gz (1 ngày, 24 file) — dùng cho docs/data_profiling.md.

Không thuộc pipeline chính thức (Bronze/Silver/Gold) — script phân tích một lần
để lấy số liệu thật, tránh việc data_profiling.md chỉ dựa trên 1 file mẫu
như schema_notes.md ở Step 2.

Usage:
    python analysis/profile_raw.py
"""

import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


def main() -> None:
    # Session riêng cho profiling (không dùng spark/session.py's 2g driver):
    # cache() một DataFrame vài trăm cột union-schema trên 4M dòng OOM ở 2g.
    spark = (
        SparkSession.builder.appName("gh-archive-profile-raw")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "6g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Liệt kê file tường minh thay vì dùng wildcard "*.json.gz": trên Windows,
    # spark.read.json("*.json.gz") gọi Hadoop globStatus() -> listStatus(),
    # và listStatus đụng NativeIO.Windows.access0 — một hàm JNI chỉ có khi có
    # winutils.exe cài đúng (HADOOP_HOME). Thiếu winutils.exe -> UnsatisfiedLinkError
    # ngay khi Spark cố liệt kê thư mục qua glob, dù đọc 1 file cụ thể vẫn hoạt
    # động bình thường (không cần listStatus).
    all_files = sorted(str(p) for p in RAW_DATA_DIR.glob("*.json.gz"))
    print(f"[profile] Đọc {len(all_files)} file trong {RAW_DATA_DIR}\n")

    df = spark.read.json(all_files)

    print("=== Số field top-level trong union schema ===")
    print(len(df.schema.fields))
    print([f.name for f in df.schema.fields])

    payload_field = [f for f in df.schema.fields if f.name == "payload"][0]
    payload_field_names = [f.name for f in payload_field.dataType.fields]
    print(f"\n=== Số field lồng bên trong payload (union tất cả event type): {len(payload_field_names)} ===")
    print(sorted(payload_field_names))

    has_corrupt = "_corrupt_record" in df.columns
    print(f"\n=== Có cột _corrupt_record không: {has_corrupt} ===")

    # Chỉ cache một PROJECTION hẹp (vài cột flat) thay vì toàn bộ union schema
    # hàng trăm cột — đây là dữ liệu thật sự cần cho hầu hết phép thống kê bên
    # dưới, nhẹ hơn nhiều lần so với cache cả payload struct đầy đủ.
    core = df.select(
        F.col("id").alias("event_id"),
        "type",
        F.col("actor.login").alias("actor_login"),
        F.col("actor.id").alias("actor_id"),
        F.col("repo.name").alias("repo_name"),
        F.col("repo.id").alias("repo_id"),
        "org",
        "public",
        "created_at",
    ).cache()

    total = core.count()
    print(f"\n[profile] Tổng số event (24 file / 1 ngày): {total:,}\n")

    print("=== Phân bố event type (cả ngày) ===")
    core.groupBy("type").count().orderBy(F.desc("count")).show(30, truncate=False)

    print("\n=== id trùng lặp? ===")
    dup = core.groupBy("event_id").count().filter("count > 1").count()
    print(f"Số id xuất hiện > 1 lần: {dup}")

    print("\n=== created_at: min / max (vẫn là string) ===")
    core.select(F.min("created_at").alias("min_ts"), F.max("created_at").alias("max_ts")).show(truncate=False)

    print("\n=== public: có giá trị false không? ===")
    core.groupBy("public").count().show()

    print("\n=== org: tỉ lệ null (event không thuộc org) ===")
    org_null = core.filter(F.col("org").isNull()).count()
    print(f"org IS NULL: {org_null:,} / {total:,} ({org_null/total:.1%})")

    print("\n=== actor / repo cardinality ===")
    print("distinct actor_login:", core.select("actor_login").distinct().count())
    print("distinct repo_name:", core.select("repo_name").distinct().count())

    print("\n=== Top 10 actor theo số event (skew check) ===")
    core.groupBy("actor_login").count().orderBy(F.desc("count")).show(10, truncate=False)

    print("\n=== Top 10 repo theo số event (skew check) ===")
    core.groupBy("repo_name").count().orderBy(F.desc("count")).show(10, truncate=False)

    if has_corrupt:
        print("\n=== _corrupt_record: số dòng JSON lỗi ===")
        bad = df.filter(F.col("_corrupt_record").isNotNull()).count()
        print(f"_corrupt_record NOT NULL: {bad:,}")
    else:
        print("\n=== Không có cột _corrupt_record — không có dòng nào bị parse lỗi. ===")

    # docs/schema_notes.md (Step 2) khẳng định payload.commits/size/distinct_size
    # tồn tại cho PushEvent — kiểm tra thực tế bằng union schema field list ở trên
    # trước khi hardcode tên field, vì GH Archive đã đổi payload schema theo thời
    # gian (không có gì đảm bảo field cũ vẫn còn).
    candidate_fields = ["push_id", "ref", "commits", "size", "distinct_size", "before", "head", "repository_id"]
    push_fields = [f for f in candidate_fields if f in payload_field_names]
    missing_fields = [f for f in candidate_fields if f not in payload_field_names]
    print(f"\n=== payload field còn tồn tại trong bộ dữ liệu này: {push_fields} ===")
    print(f"=== payload field KHÔNG tồn tại (đã bị GH Archive đổi/bỏ): {missing_fields} ===")

    print("\n=== PushEvent: null-ratio các field payload còn tồn tại ===")
    push = df.filter(F.col("type") == "PushEvent").select(
        *[F.col(f"payload.{f}").alias(f) for f in push_fields]
    ).cache()
    push_total = push.count()
    for field in push_fields:
        non_null = push.filter(F.col(field).isNotNull()).count()
        print(f"payload.{field}: non-null {non_null:,}/{push_total:,} ({non_null/push_total:.1%})")

    if "commits" in push_fields:
        print("\n=== payload.commits (PushEvent): số commit trung bình / tối đa mỗi event ===")
        push.select(F.size("commits").alias("n_commits")).describe().show()

    spark.stop()


if __name__ == "__main__":
    main()
