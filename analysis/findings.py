"""Phân tích trên Gold — đọc 3 bảng đã tổng hợp ở Step 5, in ra vài phát
hiện (findings) cho portfolio. Đây là bước ĐỌC-ONLY, không transform gì
thêm — Gold đã ở dạng sẵn sàng cho báo cáo/BI, không cần Spark xử lý nặng
ở tầng này nữa (đây chính là lý do medallion architecture tồn tại: các bước
tốn kém — parse JSON, explode, groupBy/shuffle — đã làm xong ở Bronze/
Silver/Gold, tầng này chỉ đọc lại kết quả nhỏ, rẻ).

Usage:
    python analysis/findings.py
"""

import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Side-effect: tự động set HADOOP_HOME/PATH cho Windows (xem spark/session.py).
import spark.session  # noqa: F401,E402
from pyspark.sql import SparkSession, functions as F  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GOLD_DATA_DIR = PROJECT_ROOT / "data" / "gold"


def section(title: str) -> None:
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")


def main() -> None:
    spark = (
        SparkSession.builder.appName("gh-archive-findings")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    top_repos = spark.read.parquet(str(GOLD_DATA_DIR / "top_repos_by_stars"))
    events_by_hour = spark.read.parquet(str(GOLD_DATA_DIR / "events_by_hour"))
    top_actors = spark.read.parquet(str(GOLD_DATA_DIR / "top_actors"))

    # ------------------------------------------------------------------
    section("1. Top 10 repo theo số sao (WatchEvent) trong ngày")
    # ------------------------------------------------------------------
    top_repos.orderBy(F.desc("star_count")).show(10, truncate=False)

    # ------------------------------------------------------------------
    section("2. Giờ hoạt động cao/thấp điểm nhất trong ngày")
    # ------------------------------------------------------------------
    # collect() ở đây AN TOÀN vì events_by_hour chỉ có tối đa 24 dòng (Gold
    # đã tổng hợp xong) — khác hẳn việc collect() 1 DataFrame hàng triệu
    # dòng (sẽ kéo hết dữ liệu về driver, dễ OOM). Quy mô dữ liệu quyết
    # định collect() có an toàn hay không, không phải bản thân hàm.
    hours = events_by_hour.orderBy("hour").collect()
    busiest = max(hours, key=lambda r: r["event_count"])
    quietest = min(hours, key=lambda r: r["event_count"])
    total_events = sum(r["event_count"] for r in hours)

    events_by_hour.orderBy("hour").show(24, truncate=False)
    print(f"Giờ cao điểm nhất : {busiest['hour']:02d}h — {busiest['event_count']:,} event")
    print(f"Giờ thấp điểm nhất: {quietest['hour']:02d}h — {quietest['event_count']:,} event")
    print(
        f"Chênh lệch cao/thấp điểm: {(busiest['event_count'] / quietest['event_count'] - 1) * 100:.1f}% "
        f"— hoạt động khá đều trong ngày (không có giờ 'chết' rõ rệt), phù hợp với việc "
        "GitHub có người dùng/CI toàn cầu, không tập trung 1 múi giờ."
    )

    # ------------------------------------------------------------------
    section("3. Top 10 actor active nhất (tính cả bot/automation)")
    # ------------------------------------------------------------------
    top_actors.orderBy(F.desc("event_count")).show(10, truncate=False)

    # ------------------------------------------------------------------
    section("4. Top actor active nhất — loại các tài khoản có hậu tố [bot]")
    # ------------------------------------------------------------------
    # Đây là finding thật, không phải trang trí: bảng ở mục 3 gần như toàn
    # bot (github-actions[bot], dependabot[bot]...) — lọc bỏ chúng để thấy
    # actor CON NGƯỜI/không gắn mác bot active nhất trong ngày.
    non_bot = top_actors.filter(~F.col("actor_login").endswith("[bot]")).orderBy(
        F.desc("event_count")
    )
    non_bot.show(10, truncate=False)
    print(
        "LƯU Ý (đã ghi ở docs/data_profiling.md, mục 5): nhiều login trong "
        "bảng trên có dạng chuỗi ngẫu nhiên (vd. ugmoddev, zerotraceh1, "
        "r00tsh00t12345) — nhiều khả năng vẫn là script/bot tự động, chỉ "
        "KHÔNG gắn hậu tố \"[bot]\" theo quy ước GitHub Apps. Không nên coi "
        "đây là bằng chứng chắc chắn về 'top contributor con người' nếu "
        "không xác minh thêm — chỉ nên diễn giải là 'top actor không mang "
        "mác bot rõ ràng'."
    )

    spark.stop()


if __name__ == "__main__":
    main()
