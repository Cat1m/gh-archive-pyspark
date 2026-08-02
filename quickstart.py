"""quickstart.py — 1 lệnh chạy demo end-to-end: tải 1 ngày GH Archive ->
Bronze -> Silver -> Gold -> in findings.

Đây là SCRIPT ĐIỀU PHỐI đơn giản nhất có thể — gọi lại CHÍNH XÁC các script
đã viết ở Step 1/3/4/5/7, mỗi bước chạy như 1 subprocess Python độc lập
(JVM/SparkSession riêng cho từng bước, giống hệt khi bạn tự gõ tay từng
lệnh). KHÔNG phải orchestrator thật (Airflow/Dagster) — không có retry,
scheduling, hay dependency graph — xem README.md "Future work" cho hướng
mở rộng thật. Mục đích duy nhất của file này là 1 lệnh demo cho người xem
portfolio, không phải hạ tầng production.

Usage:
    python quickstart.py                                  # tự chọn ngày hôm qua (UTC)
    python quickstart.py --start 2026-08-01 --end 2026-08-01
    python quickstart.py --skip-download                  # bỏ qua tải, dùng data/raw sẵn có
"""

import argparse
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


def run_step(title: str, script_relpath: str, extra_args: list[str] | None = None) -> None:
    print(f"\n{'#' * 70}\n# {title}\n{'#' * 70}")
    cmd = [sys.executable, script_relpath, *(extra_args or [])]
    # check=True: nếu 1 bước thất bại (vd. Bronze lỗi vì thiếu winutils.exe),
    # dừng toàn bộ quickstart ngay thay vì chạy tiếp Silver/Gold trên dữ
    # liệu Bronze không đầy đủ/không tồn tại.
    subprocess.run(cmd, check=True, cwd=str(PROJECT_ROOT))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    parser.add_argument(
        "--start", default=yesterday, help=f"Ngày bắt đầu tải, YYYY-MM-DD (mặc định: hôm qua UTC = {yesterday})"
    )
    parser.add_argument("--end", default=None, help="Ngày kết thúc (mặc định: giống --start)")
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Bỏ qua bước tải, dùng thẳng file .json.gz đã có sẵn trong data/raw/",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    end_date = args.end or args.start

    existing_raw_files = list(RAW_DATA_DIR.glob("*.json.gz"))
    if args.skip_download:
        print(f"[quickstart] --skip-download: dùng {len(existing_raw_files)} file có sẵn trong data/raw/")
    elif existing_raw_files:
        print(
            f"[quickstart] data/raw/ đã có {len(existing_raw_files)} file .json.gz — bỏ qua tải "
            "(ingestion/download_gharchive.py tự idempotent nên chạy lại cũng an toàn, "
            "nhưng quickstart mặc định không tải lại nếu đã có dữ liệu)."
        )
    else:
        run_step(
            f"Step 1: Ingestion ({args.start} -> {end_date})",
            "ingestion/download_gharchive.py",
            ["--start", args.start, "--end", end_date],
        )

    run_step("Step 3: Bronze — đọc JSON.gz, ghi Parquet partition theo event_date", "spark/bronze.py")
    run_step("Step 4: Silver — flatten + explode_outer(payload.labels)", "spark/silver.py")
    run_step("Step 5: Gold — top repo/actor, event theo giờ (shuffle)", "spark/gold.py")
    run_step("Step 7: Findings — phân tích trên Gold", "analysis/findings.py")

    print(
        "\n[quickstart] Xong! Đọc docs/spark_concepts.md để ôn lại khái niệm Spark "
        "học được ở từng step, hoặc docs/data_profiling.md/schema_notes.md để xem "
        "chi tiết dữ liệu."
    )


if __name__ == "__main__":
    main()
