"""Khám phá schema lồng nhau của 1 file GH Archive — bài học đầu tiên về
lazy evaluation: transformation nào chỉ "khai báo kế hoạch", action nào mới
thực sự khiến Spark đọc dữ liệu.

Usage:
    python spark/explore_schema.py
"""

import sys
from pathlib import Path

# Tránh crash khi in tiếng Việt có dấu trên console Windows dùng codepage
# cp1252 mặc định (cùng pattern với spark/session.py) — gap này từng bị bỏ
# sót, ghi nhận ở docs/data_profiling.md mục 7.
sys.stdout.reconfigure(encoding="utf-8")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from spark.session import get_spark_session  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


def main() -> None:
    spark = get_spark_session(app_name="gh-archive-explore-schema")

    # Chọn đại 1 file để khám phá — không cần đọc cả ngày lúc này.
    sample_file = RAW_DATA_DIR / "2026-08-01-10.json.gz"
    print(f"[explore] Đọc file mẫu: {sample_file.name}\n")

    # ------------------------------------------------------------------
    # spark.read.json(...) là TRANSFORMATION, không phải action.
    # Spark KHÔNG đọc nội dung file ở dòng này — nó chỉ ghi nhận "sau này
    # nếu có ai cần dữ liệu, đây là nguồn và đây là định dạng (JSON)". Vì
    # vậy dòng này trả về gần như ngay lập tức dù file có bao nhiêu MB.
    # (Trên thực tế Spark có "peek" một phần file để suy ra schema — nhưng
    # đây là chi phí nhỏ để lập kế hoạch, không phải xử lý toàn bộ dữ liệu.)
    # ------------------------------------------------------------------
    df = spark.read.json(str(sample_file))

    # ------------------------------------------------------------------
    # printSchema() KHÔNG phải action theo nghĩa "chạy job phân tán" — nó
    # chỉ in ra schema mà Spark đã suy luận (infer) ở bước read.json() phía
    # trên. Không có Spark job nào xuất hiện trên Spark UI vì bước này.
    # ------------------------------------------------------------------
    print("=== Schema (payload lồng nhau — thay đổi tuỳ theo loại event) ===")
    df.printSchema()

    # ------------------------------------------------------------------
    # .select(...) cũng là TRANSFORMATION — chỉ khai báo "tôi muốn lấy 2
    # cột lồng nhau này", CHƯA thực thi gì. Truy cập field lồng nhau bằng
    # cú pháp "cha.con" (actor.login, repo.name) — Spark hiểu struct type
    # và cho phép "đào sâu" vào bên trong mà không cần flatten trước.
    # ------------------------------------------------------------------
    selected = df.select("type", "actor.login", "repo.name", "created_at")

    # ------------------------------------------------------------------
    # .show() là ACTION ĐẦU TIÊN thực sự đụng tới dữ liệu trong toàn bộ
    # chuỗi ở trên. Đây là lúc Spark mới bắt đầu: mở file .json.gz, giải
    # nén, parse JSON, và chạy qua toàn bộ chain transformation phía trên
    # (read -> select) để trả về đúng 20 dòng đầu tiên bạn nhìn thấy.
    # Nếu mở Spark UI (in URL ở trên) NGAY TRƯỚC dòng này, bạn sẽ không
    # thấy job nào — job chỉ xuất hiện SAU khi dòng show() này chạy.
    # ------------------------------------------------------------------
    print("=== 20 dòng đầu (actor.login, repo.name lấy từ struct lồng nhau) ===")
    selected.show(20, truncate=False)

    # Action thứ 2: đếm số dòng — Spark phải quét lại toàn bộ file lần nữa
    # vì DataFrame là lazy và KHÔNG tự động lưu kết quả của action trước đó
    # (sẽ quay lại điểm này ở Step 6 với .cache() để tránh việc quét lặp lại).
    total = df.count()
    print(f"\n[explore] Tổng số event trong file: {total:,}")

    # Action thứ 3: liệt kê các loại event khác nhau — chính là lý do
    # "payload lồng nhau thay đổi tuỳ loại event" mà ta ghi vào docs/schema_notes.md.
    print("\n=== Các loại event (event type) xuất hiện trong file ===")
    df.groupBy("type").count().orderBy("count", ascending=False).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
