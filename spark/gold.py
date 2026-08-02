"""Gold layer — 3 bảng tổng hợp từ Silver: top repo theo star, số event theo
giờ trong ngày, top actor active nhất. Đây là nơi SHUFFLE xuất hiện lần đầu
tiên trong dự án — mọi groupBy() dưới đây đều buộc Spark trộn (shuffle) dữ
liệu qua mạng/đĩa giữa các partition.

Usage:
    python spark/gold.py
"""

import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Side-effect: tự động set HADOOP_HOME/PATH cho Windows (xem spark/session.py).
import spark.session  # noqa: F401,E402
from pyspark.sql import SparkSession, functions as F  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SILVER_DATA_DIR = PROJECT_ROOT / "data" / "silver"
GOLD_DATA_DIR = PROJECT_ROOT / "data" / "gold"

TOP_N = 20


def get_gold_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("gh-archive-gold")
        .master("local[*]")
        # spark.sql.shuffle.partitions=8: mỗi groupBy() bên dưới sẽ tạo ra
        # ĐÚNG 8 partition sau shuffle (không phải 200 mặc định) — mở Spark
        # UI, vào tab Stages của bất kỳ job groupBy nào, đếm số task ở stage
        # "reduce" (sau shuffle) sẽ luôn là 8, bất kể groupBy theo key gì.
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "6g")
        .getOrCreate()
    )
    print(f"[spark] Spark UI: {spark.sparkContext.uiWebUrl}")
    return spark


def main() -> None:
    spark = get_gold_spark_session()

    # ------------------------------------------------------------------
    # spark.read.parquet(...): TRANSFORMATION, chưa đọc gì.
    # ------------------------------------------------------------------
    silver = spark.read.parquet(str(SILVER_DATA_DIR))

    # ------------------------------------------------------------------
    # QUAN TRỌNG: grain của Silver là (event, label), KHÔNG phải (event).
    # Step 4 dùng explode_outer() trên payload.labels — 1 PullRequestEvent
    # có N label sẽ có N DÒNG TRÙNG event_id trong Silver. Các Gold table
    # dưới đây đều tính theo TỪNG EVENT (vd. "actor X có bao nhiêu event"),
    # nên nếu groupBy thẳng trên Silver chưa dedupe, 1 PR có 5 label sẽ bị
    # đếm 5 LẦN thay vì 1 — actor tác giả PR đó bị đếm sai (bội số).
    #
    # dropDuplicates(["event_id"]) đưa Silver về lại grain 1-dòng-1-event
    # trước khi tổng hợp. Đây CŨNG LÀ MỘT PHÉP GÂY SHUFFLE (Spark cần gom
    # các dòng cùng event_id về cùng 1 partition để so sánh trùng lặp) —
    # dropDuplicates về bản chất là 1 dạng groupBy ẩn, không phải phép "rẻ".
    # ------------------------------------------------------------------
    events = silver.dropDuplicates(["event_id"])

    # ==================================================================
    # GOLD TABLE 1: Top repo theo số sao (WatchEvent = hành động "star").
    #
    # .filter(...) là TRANSFORMATION, KHÔNG shuffle — Spark lọc từng dòng
    # độc lập trong phạm vi 1 partition, không cần biết dữ liệu ở partition
    # khác, nên không cần trộn dữ liệu qua mạng.
    #
    # .groupBy("repo_name").count() LÀ SHUFFLE — Spark phải đưa tất cả dòng
    # có CÙNG repo_name về cùng 1 partition (executor) để đếm gộp, bất kể
    # ban đầu 2 dòng đó nằm ở partition/file nào. Đây là lúc dữ liệu di
    # chuyển qua mạng (hoặc giữa các thread, ở chế độ local[*]) — chi phí
    # đắt hơn hẳn filter/select, và là lý do groupBy luôn xuất hiện dưới
    # dạng 1 "stage" riêng, tách biệt, trên Spark UI.
    # ==================================================================
    top_repos_by_stars = (
        events.filter(F.col("type") == "WatchEvent")
        .groupBy("repo_name")
        .count()
        .withColumnRenamed("count", "star_count")
        .orderBy(F.desc("star_count"))  # orderBy toàn cục CŨNG shuffle (cần so sánh xuyên partition)
        .limit(TOP_N)
    )

    # ==================================================================
    # GOLD TABLE 2: Số event theo từng giờ trong ngày (0-23).
    #
    # F.hour("created_at"): TRANSFORMATION, tính trên từng dòng độc lập,
    # không shuffle — giống filter/select.
    # .groupBy("hour").count(): SHUFFLE, cùng lý do như bảng 1. Vì chỉ có
    # tối đa 24 giá trị hour khác nhau, đây là ví dụ groupBy có ÍT KEY —
    # dữ liệu sẽ dồn về rất ít trong 8 partition sau shuffle (một vài
    # partition nhận nhiều hour hơn hẳn, không đều tuyệt đối), khác với
    # groupBy theo actor_login/repo_name ở bảng 3 (hàng trăm nghìn key).
    # ==================================================================
    events_by_hour = (
        events.withColumn("hour", F.hour("created_at"))
        .groupBy("hour")
        .count()
        .withColumnRenamed("count", "event_count")
        .orderBy("hour")
    )

    # ==================================================================
    # GOLD TABLE 3: Top actor active nhất (nhiều event nhất trong ngày).
    #
    # docs/data_profiling.md đã ghi nhận SKEW THẬT ở đây: github-actions[bot]
    # chiếm 5.58% tổng event toàn ngày (227,280 event) trong khi phần lớn
    # actor khác dưới 10 event. Khi groupBy("actor_login") shuffle, TOÀN BỘ
    # 227,280 dòng của github-actions[bot] phải dồn về CÙNG 1 trong 8
    # partition (vì cùng key) — 1 partition đó sẽ nặng hơn hẳn 7 partition
    # còn lại. Đây chính là "data skew": shuffle không phải lúc nào cũng
    # chia đều, mà chia THEO KEY — key nào nhiều dòng, partition đó nặng.
    # Sẽ quay lại đo cụ thể trên Spark UI (tab Stages, cột "Shuffle Read"
    # lệch hẳn giữa các task) ở Step 6.
    # ==================================================================
    top_actors = (
        events.groupBy("actor_login")
        .count()
        .withColumnRenamed("count", "event_count")
        .orderBy(F.desc("event_count"))
        .limit(TOP_N)
    )

    # ------------------------------------------------------------------
    # .write.parquet(...) x3: mỗi lệnh là 1 ACTION riêng — vì top_repos_by_stars,
    # events_by_hour, top_actors CHƯA được cache(), Spark sẽ đọc lại Silver
    # từ đầu cho MỖI action (3 lần tổng cộng), dù cả 3 đều xuất phát từ
    # cùng 1 biến `events`. Đây là ví dụ cụ thể tiếp theo cho bài học lazy
    # evaluation (Step 2) — sẽ dùng .cache() để tránh việc này ở Step 6.
    # ------------------------------------------------------------------
    print(f"[gold] Ghi 3 bảng Gold ra {GOLD_DATA_DIR}...")
    top_repos_by_stars.write.mode("overwrite").parquet(str(GOLD_DATA_DIR / "top_repos_by_stars"))
    events_by_hour.write.mode("overwrite").parquet(str(GOLD_DATA_DIR / "events_by_hour"))
    top_actors.write.mode("overwrite").parquet(str(GOLD_DATA_DIR / "top_actors"))

    print(f"\n=== GOLD TABLE 1: Top {TOP_N} repo theo số sao (WatchEvent) ===")
    top_repos_by_stars.show(TOP_N, truncate=False)

    print("\n=== GOLD TABLE 2: Số event theo giờ trong ngày ===")
    events_by_hour.show(24, truncate=False)

    print(f"\n=== GOLD TABLE 3: Top {TOP_N} actor active nhất ===")
    top_actors.show(TOP_N, truncate=False)

    print(
        "\n[gold] Mở Spark UI (URL in ở trên) -> tab Stages -> tìm 3 job "
        "groupBy vừa chạy -> so sánh \"Shuffle Read\" giữa các task trong "
        "cùng 1 stage: stage của top_actors sẽ có 1 task lệch hẳn (do "
        "github-actions[bot] dồn hết vào 1 partition) so với stage của "
        "events_by_hour (chia đều hơn nhiều, chỉ 24 key)."
    )

    spark.stop()


if __name__ == "__main__":
    main()
