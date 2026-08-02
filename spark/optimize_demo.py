"""Step 6 — 3 thí nghiệm đo THẬT (không lý thuyết suông) để cảm nhận engine
Spark: cache() tránh tính lại, spark.sql.shuffle.partitions ảnh hưởng hiệu
năng thế nào, và data skew nhìn thấy trực tiếp qua phân bố dòng theo
partition sau shuffle.

Usage:
    python spark/optimize_demo.py
"""

import sys
import time
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Side-effect: tự động set HADOOP_HOME/PATH cho Windows (xem spark/session.py).
import spark.session  # noqa: F401,E402
from pyspark.sql import SparkSession, functions as F  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SILVER_DATA_DIR = PROJECT_ROOT / "data" / "silver"


def get_optimize_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("gh-archive-optimize-demo")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "6g")
        .getOrCreate()
    )
    print(f"[spark] Spark UI: {spark.sparkContext.uiWebUrl}")
    return spark


def demo_cache(spark: SparkSession) -> None:
    print("\n" + "=" * 70)
    print("THÍ NGHIỆM 1: cache() tránh tính lại từ đầu")
    print("=" * 70)

    silver = spark.read.parquet(str(SILVER_DATA_DIR))
    # dropDuplicates(["event_id"]) đưa về grain 1-dòng-1-event (Step 5) —
    # đây là 1 phép GÂY SHUFFLE, tốn kém, nên là ứng viên tốt để minh hoạ
    # cache(): nếu không cache, MỌI action gọi sau đây đều phải chạy lại
    # toàn bộ chuỗi read -> dropDuplicates (shuffle) từ đầu.
    events = silver.dropDuplicates(["event_id"])

    # ------------------------------------------------------------------
    # KHÔNG cache: gọi 2 action liên tiếp trên cùng 1 DataFrame `events`.
    # DataFrame là LAZY và KHÔNG tự lưu kết quả của action trước đó — mỗi
    # action bên dưới là 1 job Spark HOÀN TOÀN ĐỘC LẬP, đọc lại Silver từ
    # đĩa và chạy lại dropDuplicates (shuffle) từ đầu, dù đều xuất phát từ
    # cùng 1 biến Python `events`.
    # ------------------------------------------------------------------
    t0 = time.time()
    total = events.count()
    t1 = time.time()
    push_count = events.filter(F.col("type") == "PushEvent").count()
    t2 = time.time()

    print(f"[no-cache] events.count() lần 1                    : {t1 - t0:.2f}s  (={total:,})")
    print(f"[no-cache] events.filter(PushEvent).count() lần 2   : {t2 - t1:.2f}s  (={push_count:,})")
    print(
        "[no-cache] Cả 2 dòng trên đều phải quét lại Silver + dropDuplicates "
        "từ đầu — không có gì được tái sử dụng giữa 2 action."
    )

    # ------------------------------------------------------------------
    # .cache(): TRANSFORMATION, không tính toán gì ngay lập tức — chỉ ĐÁNH
    # DẤU rằng kết quả của `events` nên được giữ lại trong bộ nhớ (RAM của
    # các executor) SAU KHI action ĐẦU TIÊN chạy xong. Bản thân dòng
    # .cache() không làm gì cho tới khi có 1 action thật sự chạy qua nó.
    # ------------------------------------------------------------------
    events_cached = events.cache()

    t3 = time.time()
    total_cached = events_cached.count()  # action ĐẦU TIÊN sau cache(): vẫn phải tính đầy đủ,
    t4 = time.time()                       # NHƯNG lần này Spark lưu lại kết quả vào bộ nhớ khi tính xong
    push_cached = events_cached.filter(F.col("type") == "PushEvent").count()  # action THỨ HAI: đọc thẳng từ cache
    t5 = time.time()

    print(f"\n[cached]   events_cached.count() lần 1 (materialize): {t4 - t3:.2f}s  (={total_cached:,})")
    print(f"[cached]   events_cached.filter(PushEvent).count() lần 2: {t5 - t4:.2f}s  (={push_cached:,})")
    print(
        "[cached]   Lần 2 nhanh hơn hẳn lần 1 — vì filter()+count() lần 2 "
        "đọc thẳng dữ liệu đã cache trong RAM, KHÔNG đọc lại Silver từ đĩa "
        "và KHÔNG chạy lại dropDuplicates (shuffle) nữa."
    )

    # unpersist(): giải phóng RAM đã cache — làm sạch trước khi sang thí
    # nghiệm tiếp theo, tránh 1 DataFrame lớn chiếm RAM không cần thiết.
    events_cached.unpersist()


def demo_shuffle_partitions(spark: SparkSession) -> None:
    print("\n" + "=" * 70)
    print("THÍ NGHIỆM 2: spark.sql.shuffle.partitions ảnh hưởng hiệu năng")
    print("=" * 70)

    silver = spark.read.parquet(str(SILVER_DATA_DIR))
    events = silver.dropDuplicates(["event_id"]).cache()
    events.count()  # materialize cache 1 lần, để 2 phép đo dưới đây công bằng
    # (không tính luôn cả thời gian đọc Silver vào phép đo shuffle partitions)

    # spark.conf.set(...) đổi ĐƯỢC giữa chừng, không cần tạo lại SparkSession
    # — mỗi query sau đó sẽ dùng giá trị mới nhất tại thời điểm action chạy.
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    t0 = time.time()
    n_groups_200 = events.groupBy("actor_login").count().count()
    t1 = time.time()

    spark.conf.set("spark.sql.shuffle.partitions", "8")
    t2 = time.time()
    n_groups_8 = events.groupBy("actor_login").count().count()
    t3 = time.time()

    print(f"[shuffle.partitions=200] groupBy(actor_login).count(): {t1 - t0:.2f}s  ({n_groups_200:,} actor khác nhau)")
    print(f"[shuffle.partitions=8]   groupBy(actor_login).count(): {t3 - t2:.2f}s  ({n_groups_8:,} actor khác nhau)")
    print(
        "[nhận xét] 200 partition nghĩa là Spark phải lên lịch (schedule) "
        "200 task nhỏ cho MỖI stage sau shuffle, mỗi task có chi phí khởi "
        "động cố định (vài chục ms) bất kể task đó xử lý bao nhiêu dòng. "
        "Với dữ liệu demo ~4 triệu dòng chạy local[*] (CPU core hữu hạn), "
        "200 task nhỏ tạo overhead lớn hơn hẳn 8 task — trên cluster nhiều "
        "máy, dữ liệu hàng terabyte, 200 (hoặc hơn) mới hợp lý vì mỗi task "
        "khi đó xử lý đủ nhiều dữ liệu để bù lại chi phí khởi động."
    )

    events.unpersist()


def demo_data_skew(spark: SparkSession) -> None:
    print("\n" + "=" * 70)
    print("THÍ NGHIỆM 3: data skew — nhìn thấy trực tiếp qua phân bố partition")
    print("=" * 70)

    silver = spark.read.parquet(str(SILVER_DATA_DIR))
    events = silver.dropDuplicates(["event_id"])

    # groupBy("actor_login").count(): sau shuffle, mỗi actor_login (key) đi
    # về ĐÚNG 1 trong spark.sql.shuffle.partitions partition — Spark dùng
    # hash(key) % số_partition để quyết định, nên CÙNG 1 key LUÔN về CÙNG 1
    # partition. Vấn đề: hash chia đều SỐ LƯỢNG KEY vào các partition,
    # KHÔNG chia đều SỐ LƯỢNG DÒNG — nếu 1 key (github-actions[bot]) có
    # 227,280 dòng còn phần lớn key khác dưới 10 dòng, partition chứa key
    # đó sẽ "nặng" hơn hẳn, dù số actor (số key) rải ra tương đối đều.
    #
    # F.spark_partition_id(): hàm built-in trả về ID của partition vật lý
    # (0..N-1) mà 1 dòng đang nằm — dùng để "nhìn xuyên" vào bên trong
    # DataFrame, thấy dữ liệu THỰC SỰ nằm ở đâu sau shuffle.
    per_actor_count = events.groupBy("actor_login").count()

    per_partition = (
        per_actor_count.withColumn("partition_id", F.spark_partition_id())
        .groupBy("partition_id")
        .agg(
            F.count("*").alias("so_actor_trong_partition"),
            F.sum("count").alias("tong_so_dong_goc"),
            F.max("count").alias("actor_dong_nhieu_nhat_trong_partition"),
        )
        .orderBy("partition_id")
    )

    print(
        "Mỗi dòng dưới đây là 1 trong 8 partition SAU shuffle của "
        "groupBy(\"actor_login\"). Cột \"tong_so_dong_goc\" là tổng số EVENT "
        "GỐC (không phải số actor) rơi vào partition đó — đây là con số "
        "phơi bày skew, không phải \"so_actor_trong_partition\" (số actor "
        "vẫn có thể tương đối đều)."
    )
    per_partition.show(truncate=False)

    top_actor_row = (
        per_actor_count.orderBy(F.desc("count")).limit(1).collect()[0]
    )
    print(
        f"[skew thật] Actor active nhất: {top_actor_row['actor_login']} "
        f"với {top_actor_row['count']:,} event — nằm gọn trong 1 partition "
        "duy nhất ở bảng trên. Nếu bạn thấy 1 dòng có tong_so_dong_goc cao "
        "vượt trội hẳn 7 dòng còn lại, đó chính là partition đang gánh "
        "phần lớn công việc của cả stage — các task khác đã xong từ lâu "
        "trong khi task đó vẫn đang chạy (thấy rõ trên Spark UI, tab Stages, "
        "task đó có thời gian chạy dài hơn hẳn 7 task anh em)."
    )


def main() -> None:
    spark = get_optimize_spark_session()

    demo_cache(spark)
    demo_shuffle_partitions(spark)
    demo_data_skew(spark)

    spark.stop()


if __name__ == "__main__":
    main()
