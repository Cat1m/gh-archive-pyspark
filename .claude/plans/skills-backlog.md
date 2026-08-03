# Backlog: 6 Claude Skills hỗ trợ workflow Data Engineering

## Context

Sau khi thảo luận về cách DE có kinh nghiệm dùng Claude Skills để chuẩn hoá
quy trình lặp lại và encode tribal knowledge (xem hội thoại gốc), đã chốt 6
skill đáng viết cho project này. Đây là backlog để tham khảo khi có thời
gian viết — chưa cần triển khai ngay ("chưa cần đâu ạ").

Skill trong Claude Code là 1 bộ hướng dẫn đóng gói, đặt tại
`.claude/skills/<tên>/SKILL.md` (theo cấu trúc skill hiện có của dự án, xem
các skill built-in như `commit-message`, `security-review` để tham khảo
format).

## 1. `spark-review` — Checklist review kỹ thuật Spark

**Vấn đề giải quyết**: đảm bảo review code Spark nhất quán bất kể ai review,
dựa trên đúng các bẫy đã tự phát hiện và ghi chú kỹ trong
`spark/silver.py`/`gold.py`/`optimize_demo.py`.

**Nội dung checklist đề xuất**:
- Thiếu `.cache()` trước khi có ≥2 action trên cùng DataFrame.
- Dùng `explode()` thay vì `explode_outer()` cho mảng có thể NULL.
- `groupBy`/`dropDuplicates` không có `spark.sql.shuffle.partitions` phù
  hợp quy mô data.
- Dedup sai grain (dedup theo key thiếu cột, xoá nhầm dòng hợp lệ — như bug
  class đã ghi trong `gold.py` dòng 53-64).
- `withColumn`/`select` lồng transformation nặng mà không có comment giải
  thích lazy vs action.

## 2. `add-new-source` — Scaffold nguồn dữ liệu Bronze mới

**Vấn đề giải quyết**: mỗi khi thêm 1 nguồn dữ liệu mới (khác GH Archive),
tự động sinh ingestion script + Bronze reader theo đúng convention hiện có
(idempotent + retry như `ingestion/download_gharchive.py`, partition theo
ngày như `spark/bronze.py`), tránh mỗi lần làm khác nhau.

**Nội dung đề xuất**: template ingestion script (idempotent skip-if-exists,
retry với backoff), template Bronze reader (đọc list file tường minh thay
vì wildcard — tránh bug Windows `listStatus` đã gặp), checklist đặt tên
partition column.

## 3. `profile-new-source` — Chuẩn hoá data profiling

**Vấn đề giải quyết**: mỗi khi có nguồn dữ liệu mới, chạy đúng 1 quy trình
profiling (null rate, cardinality, schema drift so với lần trước) và ghi ra
doc theo cùng format — giống cách `docs/data_profiling.md` đã làm thủ công,
nhưng lặp lại được tự động.

**Nội dung đề xuất**: quy trình chạy `spark.read.json()` + tính null % mỗi
cột + so sánh field mới/mất so với schema đã biết trước đó + ghi kết quả
vào file `docs/data_profiling_<source>.md` theo template sẵn.

## 4. `add-transform-test` — Sinh test scaffolding cho transform logic

**Vấn đề giải quyết**: liên quan trực tiếp Priority 1.1
([[priority-1-core-de-signals]]) — mỗi khi thêm transformation mới ở
Silver/Gold, tự sinh test case theo pattern đã chốt (assert row count không
đổi ngoài ý muốn, assert đúng grain, assert dedup key đúng) thay vì viết
tay lại từ đầu mỗi lần.

**Nội dung đề xuất**: template pytest dùng SparkSession `local[1]` +
DataFrame giả lập nhỏ, tự sinh 3 loại assert cơ bản (schema, row count
trước/sau transform, grain/dedup key).

## 5. `debug-pipeline-runbook` — Quy trình xử lý sự cố pipeline

**Vấn đề giải quyết**: encode kinh nghiệm debug thành quy trình lặp lại
được, giảm phụ thuộc vào 1 người biết rõ hệ thống — hữu ích nhất sau khi có
orchestration thật ([[priority-2-engineering-practices]] mục 2.1).

**Nội dung đề xuất**: thứ tự kiểm tra khi 1 bước pipeline fail (check log
Spark UI/Airflow task log → lỗi thường gặp đã biết của project này, vd.
`UnsatisfiedLinkError` thiếu winutils, OOM driver do union schema lớn →
bước khôi phục an toàn, không rerun đè lên data tốt).

## 6. Onboarding kỹ sư mới

**Lưu ý**: Claude Code đã có sẵn cơ chế `ONBOARDING.md` +
`ShareOnboardingGuide` cho việc này — có thể KHÔNG cần viết skill riêng, chỉ
cần soạn nội dung `ONBOARDING.md` cho project và dùng tool có sẵn để chia
sẻ. Chỉ cân nhắc viết thành skill riêng nếu muốn tự động hoá việc tạo lại
onboarding doc mỗi khi kiến trúc project thay đổi đáng kể (vd. sau khi thêm
Delta Lake, orchestration — [[priority-4-polish]]).

## Ghi chú thứ tự khi bắt tay viết

Không xếp priority cứng vì cả 6 đều nhỏ và độc lập, nhưng gợi ý logic:
viết `spark-review` và `add-transform-test` trước (giá trị ngay, gắn liền
Priority 1 đang làm), `add-new-source`/`profile-new-source` khi thực sự có
nguồn dữ liệu thứ 2 cần thêm, `debug-pipeline-runbook` sau khi có
orchestration thật ở Priority 2.
