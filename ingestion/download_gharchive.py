"""Tải dữ liệu GH Archive (.json.gz, mỗi giờ 1 file) theo khoảng ngày.

Đây là bước Python THUẦN (requests + filesystem) — CHƯA đụng tới Spark.
Ranh giới rõ ràng: ingestion (tải file thô về đĩa qua mạng) tách biệt khỏi
processing (đọc/biến đổi dữ liệu bằng Spark, sẽ làm ở Step 2+). Tách vậy để
có thể tải data 1 lần, rồi chạy đi chạy lại các thử nghiệm Spark trên cùng
1 bộ dữ liệu mà không cần tải lại.

Usage:
    python ingestion/download_gharchive.py --start 2024-01-01 --end 2024-01-01
"""

import argparse
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

# Cùng lý do như spark/session.py: tránh crash khi in ký tự đặc biệt trên
# console Windows dùng codepage cp1252 mặc định.
sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"

GHARCHIVE_BASE_URL = "https://data.gharchive.org"

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5


def download_hour_file(date: datetime, hour: int) -> Path:
    """Tải 1 file .json.gz cho đúng 1 giờ, skip nếu đã tồn tại (idempotent).

    GH Archive đặt tên file theo giờ KHÔNG zero-pad (vd. "...-5.json.gz" chứ
    không phải "...-05.json.gz"), khác với ngày/tháng luôn có 2 chữ số.
    """
    filename = f"{date.strftime('%Y-%m-%d')}-{hour}.json.gz"
    dest_path = RAW_DATA_DIR / filename

    if dest_path.exists():
        print(f"[skip] {filename} đã tồn tại, không tải lại")
        return dest_path

    url = f"{GHARCHIVE_BASE_URL}/{filename}"
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"[download] {url} -> {filename} (lần {attempt}/{MAX_RETRIES})")
            # stream=True: không load toàn bộ response vào RAM cùng lúc — quan
            # trọng vì mỗi file GH Archive có thể vài chục MB và script này
            # sẽ tải hàng chục file liên tiếp.
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()

            with open(dest_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)

            return dest_path

        except (requests.RequestException, IOError) as exc:
            last_error = exc
            # Xóa file lỗi dở (nếu có) để lần retry sau không nhầm là đã tải xong.
            if dest_path.exists():
                dest_path.unlink()
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF_SECONDS * attempt
                print(f"[retry] Lỗi mạng: {exc} — chờ {wait}s rồi thử lại")
                time.sleep(wait)

    raise RuntimeError(f"Tải thất bại sau {MAX_RETRIES} lần: {url}") from last_error


def download_range(start_date: datetime, end_date: datetime) -> list[Path]:
    """Tải tất cả file giờ (0-23) cho mọi ngày trong [start_date, end_date]."""
    downloaded = []
    current = start_date
    while current <= end_date:
        for hour in range(24):
            path = download_hour_file(current, hour)
            downloaded.append(path)
        current += timedelta(days=1)
    return downloaded


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--start",
        required=True,
        help="Ngày bắt đầu, định dạng YYYY-MM-DD",
    )
    parser.add_argument(
        "--end",
        required=True,
        help="Ngày kết thúc (bao gồm), định dạng YYYY-MM-DD",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d")

    if end_date < start_date:
        raise ValueError("--end phải >= --start")

    num_days = (end_date - start_date).days + 1
    print(f"[info] Tải GH Archive từ {args.start} đến {args.end} ({num_days} ngày, {num_days * 24} file)")

    downloaded = download_range(start_date, end_date)

    print(f"\n[done] Hoàn tất: {len(downloaded)} file trong {RAW_DATA_DIR}")


if __name__ == "__main__":
    main()
