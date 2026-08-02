"""Chạy 1 job groupBy thật trên Silver rồi GIỮ Spark UI sống cho tới khi
bạn nhấn Enter — dùng để mở trình duyệt chụp Spark UI (tab Jobs/Stages)
cho README, không phải 1 phần của pipeline chính thức.

Usage:
    python spark/keep_ui_alive.py
"""

import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import spark.session  # noqa: F401,E402
from pyspark.sql import SparkSession, functions as F  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SILVER_DATA_DIR = PROJECT_ROOT / "data" / "silver"


def main() -> None:
    spark = (
        SparkSession.builder.appName("gh-archive-ui-screenshot")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "6g")
        .getOrCreate()
    )
    print(f"[spark] Spark UI: {spark.sparkContext.uiWebUrl}")

    silver = spark.read.parquet(str(SILVER_DATA_DIR))
    events = silver.dropDuplicates(["event_id"]).cache()
    events.count()
    events.groupBy("actor_login").count().orderBy(F.desc("count")).show(10, truncate=False)
    events.groupBy(F.hour("created_at").alias("hour")).count().orderBy("hour").show(24, truncate=False)

    print("\n>>> Mở trình duyệt tại URL Spark UI ở trên, vào tab Jobs/Stages để chụp.")
    input(">>> Nhấn Enter tại đây khi chụp xong để đóng Spark UI... ")

    spark.stop()


if __name__ == "__main__":
    main()
