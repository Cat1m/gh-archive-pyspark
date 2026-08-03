# Priority 1 — Ảnh hưởng lớn nhất đến tín hiệu "data engineer thật"

## Context

Project hiện tại (Bronze/Silver/Gold local trên GH Archive data) đã mạnh về
hiểu sâu engine Spark (lazy eval, shuffle, cache, quan sát skew bằng số liệu
thật) nhưng còn thiếu chiều rộng của một data engineer thực thụ. Khảo sát
`docs/spark_concepts.md`, `spark/optimize_demo.py` xác nhận: salting/AQE
được nêu tên nhưng chốt "out of scope"; không có pytest nào trong repo;
README chỉ tiếng Việt. Đây là 3 hạng mục có ảnh hưởng lớn nhất khi người
review (nhà tuyển dụng) đánh giá portfolio.

## 1.1 Tự động hoá test cho transformation logic

Hiện tại verify chỉ là in số dòng ra console rồi tự so sánh bằng mắt (Step
3/4 trong `docs/spark_concepts.md`) — không fail-fast, không tái sử dụng
được. Thêm `pytest` + SparkSession fixture nhỏ (`local[1]`) để assert:

- Silver: `explode_outer` không làm mất dòng non-PR (so tổng dòng Bronze vs
  distinct `event_id` ở Silver).
- Grain đúng: dedup key `(event_id, pr_label_name)` không xoá nhầm label hợp
  lệ, dùng data giả lập nhỏ (không cần chạy trên 4M dòng thật).
- Gold: `dropDuplicates(["event_id"])` trước groupBy không làm actor có PR
  nhiều label bị đếm bội số (regression test trực tiếp cho bug class đã ghi
  chú kỹ trong `spark/gold.py`).

File liên quan: `spark/silver.py`, `spark/gold.py`. Thêm `pytest` vào
`requirements.txt`, tạo thư mục `tests/`.

## 1.2 Thực sự xử lý data skew (không chỉ quan sát)

`spark/optimize_demo.py` đã đo và chứng minh skew của `github-actions[bot]`
bằng `spark_partition_id()`, nhưng dừng lại ở quan sát — `spark_concepts.md`
chốt thẳng "out of scope". Đây là khoảng trống dễ thấy nhất với người review
kỹ: đã CHỈ RA vấn đề nhưng chưa GIẢI QUYẾT nó. Đề xuất thêm 1 phần trong
`spark/optimize_demo.py` (hoặc file mới `spark/skew_fix_demo.py`):

- Bật `spark.sql.adaptive.enabled` + `spark.sql.adaptive.skewJoin.enabled`
  và so sánh lại phân bố partition/thời gian trước-sau (dễ làm nhất, ít
  code).
- Hoặc salting thủ công cho key `actor_login` trước `groupBy`, so sánh phân
  bố `spark_partition_id()` trước/sau — minh hoạ kỹ thuật salting thật,
  không chỉ nhắc tên.

## 1.3 README song ngữ (ít nhất bản tiếng Anh)

README hiện tại (findings, kiến trúc, Spark UI walkthrough) chỉ có tiếng
Việt — hạn chế tiếp cận nếu portfolio nhắm nhà tuyển dụng/công ty quốc tế.
Không cần dịch toàn bộ ngay — tạo `README.en.md` link chéo với `README.md`,
ưu tiên dịch phần Kiến trúc, Findings, "Điều tôi học được về Spark" (phần
có giá trị nhất với người đánh giá).

## Verification

- 1.1: `pytest tests/` chạy xanh, không cần data thật (dùng SparkSession
  `local[1]` + DataFrame giả lập nhỏ).
- 1.2: So sánh output `spark_partition_id()` distribution trước/sau khi bật
  AQE hoặc salting — chênh lệch max-min phải giảm rõ rệt so với baseline đã
  đo trong `optimize_demo.py` hiện tại.
