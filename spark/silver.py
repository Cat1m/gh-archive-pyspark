"""Silver layer — flatten field lồng nhau, explode payload.labels
(PullRequestEvent), chuẩn hoá timestamp, từ Bronze Parquet.

Bối cảnh quyết định (xem docs/data_profiling.md §4.1): kế hoạch gốc là
explode PushEvent.payload.commits, nhưng field đó không tồn tại trong dữ
liệu thật (GH Archive đã bỏ). Thay bằng PullRequestEvent.payload.labels —
array<struct> có thật, 1,336/3,891 PullRequestEvent có data, avg 2.37 label,
max 43 label/PR.

Usage:
    python spark/silver.py
"""

import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Side-effect: tự động set HADOOP_HOME/PATH cho Windows (xem spark/session.py).
import spark.session  # noqa: F401,E402
from pyspark.sql import SparkSession, functions as F  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_DATA_DIR = PROJECT_ROOT / "data" / "bronze"
SILVER_DATA_DIR = PROJECT_ROOT / "data" / "silver"

CREATED_AT_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"


def get_silver_spark_session() -> SparkSession:
    """Session riêng, driver.memory=6g — cùng lý do như spark/bronze.py:
    đọc lại Bronze vẫn là union schema đầy đủ (hàng trăm cột lồng nhau)
    trước khi Silver select() thu hẹp lại, nên driver 2g mặc định của
    spark/session.py không đủ (xem docs/data_profiling.md §7).
    """
    spark = (
        SparkSession.builder.appName("gh-archive-silver")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "6g")
        .getOrCreate()
    )
    print(f"[spark] Spark UI: {spark.sparkContext.uiWebUrl}")
    return spark


def main() -> None:
    spark = get_silver_spark_session()

    # ------------------------------------------------------------------
    # spark.read.parquet(...): TRANSFORMATION. Không đọc byte nào ở đây —
    # Parquet lưu kèm schema + thống kê (min/max, null count) trong metadata
    # của chính file, nên bước này còn rẻ hơn cả spark.read.json() ở Bronze
    # (không cần "peek" dữ liệu để suy luận schema như JSON).
    # ------------------------------------------------------------------
    bronze = spark.read.parquet(str(BRONZE_DATA_DIR))

    # ------------------------------------------------------------------
    # FLATTEN: select() các field lồng nhau (actor.login, repo.name, ...)
    # ra thành cột phẳng cấp cao nhất, đặt tên rõ ràng (actor_login thay vì
    # actor.login). Đây vẫn là TRANSFORMATION — chỉ khai báo "tôi muốn những
    # cột này, đặt tên thế này", Spark chưa tính toán gì.
    #
    # org.login: giữ nguyên NULL cho ~96% event không thuộc tổ chức nào
    # (xác nhận ở docs/data_profiling.md §2) — KHÔNG lọc bỏ, vì null ở đây
    # là dữ liệu thật (event cá nhân), không phải rác.
    #
    # payload.labels: array<struct> — nhưng KHÔNG chỉ PullRequestEvent dùng
    # field này. docs/data_profiling.md §"Explode strategy" đã khảo sát:
    # IssuesEvent CŨNG populate payload.labels (530 event, avg 3.13 label),
    # vì GH Archive dùng chung 1 field "labels" cho cả 2 loại event ở tầng
    # union schema. Quyết định đã chốt là dùng ví dụ explode CHỈ cho
    # PullRequestEvent — nên phải ép NULL field này ở mọi type khác (kể cả
    # IssuesEvent), bằng F.when(...), thay vì lấy thẳng payload.labels. Nếu
    # không, cột pr_label_name (đặt tên ngụ ý "chỉ PR") sẽ âm thầm lẫn cả
    # label của IssuesEvent — sai với tên cột và sai phạm vi đã quyết định.
    # ------------------------------------------------------------------
    flattened = bronze.select(
        F.col("id").alias("event_id"),
        F.col("type"),
        F.col("actor.id").alias("actor_id"),
        F.col("actor.login").alias("actor_login"),
        F.col("repo.id").alias("repo_id"),
        F.col("repo.name").alias("repo_name"),
        F.col("org.login").alias("org_login"),
        F.col("public"),
        # to_timestamp với format tường minh: created_at ở Bronze vẫn là
        # STRING (Bronze giữ nguyên raw, không cast) — Silver là nơi chốt
        # kiểu dữ liệu thật cho các bước dùng tiếp theo (Gold sẽ group theo
        # giờ, ngày... cần kiểu timestamp, không phải string).
        F.to_timestamp(F.col("created_at"), CREATED_AT_FORMAT).alias("created_at"),
        F.col("event_date"),
        F.when(F.col("type") == "PullRequestEvent", F.col("payload.labels"))
        .otherwise(F.lit(None))
        .alias("pr_labels"),
    )

    # ------------------------------------------------------------------
    # explode_outer() — ĐÂY LÀ TRỌNG TÂM CỦA STEP 4.
    #
    # explode_outer("pr_labels") biến 1 dòng có mảng N phần tử thành N
    # dòng riêng biệt, mỗi dòng giữ 1 phần tử của mảng — số dòng của
    # DataFrame THAY ĐỔI sau bước này (không giữ nguyên 1-1 như mọi
    # transformation trước đó trong file này). Ví dụ: 1 PullRequestEvent
    # có 5 label sẽ trở thành 5 dòng, mỗi dòng khác nhau ở cột label,
    # nhưng giống hệt nhau ở các cột còn lại (event_id, actor_login...).
    #
    # TẠI SAO DÙNG explode_outer() THAY VÌ explode() THƯỜNG:
    # explode() (không có "_outer") sẽ XOÁ LUÔN dòng nào có giá trị mảng là
    # NULL hoặc mảng rỗng. Vì pr_labels chỉ có giá trị ở PullRequestEvent
    # (mọi loại event khác — PushEvent, WatchEvent, IssuesEvent... — đều có
    # pr_labels = NULL), dùng explode() thường sẽ ÂM THẦM XOÁ TOÀN BỘ
    # 4,071,556 - 3,891 dòng không phải PullRequestEvent, và cả những
    # PullRequestEvent không có label nào! explode_outer() giữ lại 1 dòng
    # (với cột label = NULL) cho mọi trường hợp mảng NULL/rỗng — đúng ý
    # định của Silver ở đây: có thêm dữ liệu label khi có, KHÔNG được xoá
    # mất các event khác chỉ vì chúng không có label.
    # ------------------------------------------------------------------
    exploded = flattened.withColumn("pr_label", F.explode_outer(F.col("pr_labels"))).drop(
        "pr_labels"
    )

    silver = exploded.select(
        "event_id",
        "type",
        "actor_id",
        "actor_login",
        "repo_id",
        "repo_name",
        "org_login",
        "public",
        "created_at",
        "event_date",
        F.col("pr_label.name").alias("pr_label_name"),
        F.col("pr_label.color").alias("pr_label_color"),
    )

    # ------------------------------------------------------------------
    # dropDuplicates: docs/data_profiling.md đã xác nhận 0 event_id trùng
    # lặp trong dữ liệu thật — bước này gần như KHÔNG xoá dòng nào, không
    # phải sửa lỗi đã phát hiện. Giữ lại như 1 bước phòng thủ tường minh
    # (idempotency khi Silver có thể chạy lại nhiều lần trên cùng Bronze).
    #
    # Key dedup là (event_id, pr_label_name), KHÔNG chỉ event_id — vì sau
    # explode_outer(), 1 PullRequestEvent có N label sẽ hợp lệ xuất hiện N
    # lần với cùng event_id nhưng khác pr_label_name. Nếu dedupe chỉ theo
    # event_id, sẽ xoá nhầm N-1 dòng label hợp lệ, tưởng là "trùng lặp".
    # ------------------------------------------------------------------
    silver_clean = silver.dropDuplicates(["event_id", "pr_label_name"])

    # ------------------------------------------------------------------
    # .write.parquet(...): ACTION — chạy toàn bộ chuỗi read -> flatten ->
    # explode_outer -> dedupe ở trên. partitionBy("event_date") giữ nguyên
    # convention từ Bronze (Step 3).
    # ------------------------------------------------------------------
    print(f"[silver] Ghi Parquet ra {SILVER_DATA_DIR}, partition theo event_date...")
    silver_clean.write.mode("overwrite").partitionBy("event_date").parquet(
        str(SILVER_DATA_DIR)
    )

    # Verify: đọc lại và so số liệu để thấy rõ tác động của explode_outer.
    written = spark.read.parquet(str(SILVER_DATA_DIR))
    total_rows = written.count()
    distinct_events = written.select("event_id").distinct().count()
    pr_rows_with_label = written.filter(F.col("pr_label_name").isNotNull()).count()

    print(f"\n[silver] Tổng số dòng sau explode_outer: {total_rows:,}")
    print(f"[silver] Số event_id khác nhau (không đổi so với Bronze): {distinct_events:,}")
    print(f"[silver] Số dòng có pr_label_name (từ PullRequestEvent có label): {pr_rows_with_label:,}")
    print(
        "[silver] Chênh lệch (tổng dòng - số event gốc) = số label 'thừa ra' "
        f"do 1 PR có nhiều label: {total_rows - distinct_events:,}"
    )

    print("\n=== 10 dòng mẫu có pr_label_name (minh hoạ 1 PR -> nhiều dòng) ===")
    written.filter(F.col("pr_label_name").isNotNull()).select(
        "event_id", "repo_name", "pr_label_name", "pr_label_color"
    ).orderBy("event_id").show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
