# Priority 4 — Polish / tuỳ chọn

## Context

Các hạng mục còn lại có giá trị nhưng ít liên quan trực tiếp đến việc "hiểu
Spark engine" hơn Priority 1-3 ([[priority-1-core-de-signals]],
[[priority-2-engineering-practices]], [[priority-3-scale-and-depth]]) — phù
hợp làm sau cùng hoặc bỏ qua nếu thời gian hạn chế.

## 4.1 CLI argument cho các script thay vì hardcode

`spark/bronze.py`/`silver.py`/`gold.py` không nhận argument (chỉ
`download_gharchive.py` và `quickstart.py` có `argparse`) — thêm để dễ chạy
trên nhiều path/ngày khác nhau mà không sửa code, đi kèm với mục 2.2 ở
[[priority-2-engineering-practices]].

## 4.2 Delta Lake/Iceberg thay Parquet thô

Đã có trong README "Future work" của chính user — ACID + time travel, giá
trị portfolio cao nhưng ít liên quan trực tiếp đến việc hiểu Spark engine
hơn các priority trên, nên xếp thấp nhất.

## 4.3 Dọn `notebooks/` (hiện rỗng)

Hoặc thêm 1 notebook EDA ngắn (khám phá schema/findings bằng
Jupyter+matplotlib, thân thiện hơn console output cho người xem portfolio
không rành Spark), hoặc xoá thư mục rỗng nếu không có kế hoạch dùng.

## Verification

- 4.1: Chạy `python spark/bronze.py --raw-dir <custom> --out-dir <custom>`
  (hoặc tương đương) và xác nhận ghi đúng path chỉ định, không đụng
  `data/bronze` mặc định.
- 4.2/4.3: Không có tiêu chí đo lường bắt buộc — đánh giá bằng mắt khi
  review portfolio.
