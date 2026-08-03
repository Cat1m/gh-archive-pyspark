# Priority 3 — Mở rộng chiều sâu/quy mô

## Context

Toàn bộ pipeline hiện chỉ test trên đúng 1 ngày dữ liệu (~4M event,
~490MB) và chỉ chạy trên `local[*]`, chưa từng chạm cluster thật. Nhóm hạng
mục này mở rộng bằng chứng "đã kiểm chứng ở quy mô lớn hơn / môi trường
thật", bổ sung cho phần lý thuyết engine đã vững ở Priority 1
([[priority-1-core-de-signals]]).

## 3.1 Chạy thử ở quy mô lớn hơn 1 ngày

Hiện tại chưa thấy rõ giá trị của `partitionBy(event_date)` (partition
pruning chỉ có ý nghĩa khi có nhiều ngày để pruning). Chạy thử 7-30 ngày
liên tục, đo lại Bronze/Silver/Gold ở quy mô đó, cập nhật
`docs/data_profiling.md` với số liệu mới — biến "thiết kế đúng" thành "đã
kiểm chứng đúng".

## 3.2 Data quality checks tự động (không chỉ tests logic)

Bổ sung layer kiểm tra chất lượng dữ liệu chạy như 1 bước riêng (không phải
unit test) — ví dụ: row count reconciliation Bronze→Silver→Gold tự động
assert và fail loudly thay vì chỉ print, cảnh báo nếu tỷ lệ null của 1 cột
tăng đột biến so với baseline đã ghi trong `docs/data_profiling.md`.

## 3.3 Chạy thử 1 lần trên cluster thật (Databricks Community Edition)

Toàn bộ kinh nghiệm hiện tại là `local[*]`. Chạy lại pipeline (hoặc 1 phần)
trên Databricks Community Edition/free tier 1 lần, chụp lại Spark UI
cluster thật, ghi chú khác biệt (network shuffle, executor thật vs thread
local) — đủ để trả lời tự tin câu hỏi phỏng vấn "bạn có kinh nghiệm cluster
không".

## Verification

- 3.1/3.3: Số liệu mới trong `docs/data_profiling.md` phải nhất quán về đơn
  vị/format với số liệu 1-ngày hiện có để so sánh được.
- 3.2: Chạy pipeline với 1 file raw bị lỗi/thiếu cố ý, xác nhận quality
  check phát hiện và báo lỗi rõ ràng thay vì âm thầm ghi ra Gold sai số
  liệu.
