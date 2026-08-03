# Priority 2 — Giá trị bổ sung mạnh (engineering practices)

## Context

Sau khi giải quyết Priority 1 ([[priority-1-core-de-signals]]), nhóm hạng
mục này nâng chất lượng kỹ thuật của pipeline: hiện `quickstart.py` tự nhận
là "subprocess wrapper đơn giản nhất có thể" (không retry/scheduling), các
constant (`shuffle.partitions`, `driver.memory`, đường dẫn) hardcode rải rác
mỗi script, và toàn bộ logging chỉ là `print(f"[bronze] ...")`.

## 2.1 Orchestration tối thiểu thay `quickstart.py`

Thêm 1 DAG Airflow (hoặc Dagster) tối thiểu — dù chỉ chạy local với
`LocalExecutor`/`SequentialExecutor` — map đúng 4 task hiện có (ingest →
bronze → silver → gold) là đủ để chứng minh hiểu orchestration thật, không
cần production-grade. Giữ `quickstart.py` như fallback cho người không cài
Airflow.

## 2.2 Tập trung config thay vì hardcode rải rác

`spark.sql.shuffle.partitions=8`, `driver.memory` (2g vs 6g tuỳ script),
đường dẫn `data/raw`, `data/bronze`... đang lặp lại và hardcode riêng ở
từng file (`spark/bronze.py`, `silver.py`, `gold.py`, `session.py`). Gom
vào 1 `config.py` hoặc file `.env`/YAML — giảm trùng lặp, dễ chỉnh khi scale
lên data lớn hơn (xem [[priority-3-scale-and-depth]] mục 3.1).

## 2.3 Logging có cấu trúc thay vì `print`

Toàn bộ project dùng `print(f"[bronze] ...")` — dùng được cho demo nhưng
không phải thói quen engineering chuẩn. Đổi sang `logging` module (level
INFO/WARNING, format có timestamp) — thay đổi nhỏ nhưng dễ thấy khi review
code.

## Verification

- 2.1: DAG chạy được end-to-end qua Airflow UI/CLI, thay thế hoàn toàn
  `quickstart.py` cho luồng chính.
- 2.2/2.3: Chạy lại toàn bộ pipeline (`python quickstart.py --skip-download`)
  sau khi refactor, output/row-count phải khớp với trước khi đổi.
