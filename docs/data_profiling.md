# Data Profiling — data/raw (2026-08-01, 24 file)

Số liệu thật, chạy trên **toàn bộ ngày** (24 file `.json.gz`, 480MB, 4,071,556
event), khác với `schema_notes.md` (Step 2) vốn chỉ dựa trên 1 file mẫu / 1 giờ
(169,582 event). Script: `analysis/profile_raw.py`.

Mục tiêu: cho Step 3-6 (Bronze/Silver/Gold) một bức tranh đúng về dữ liệu thật
trước khi thiết kế transform, thay vì suy diễn từ 1 giờ mẫu.

## 1. Quy mô

| | |
|---|---|
| Số file | 24 (1 file / giờ, `2026-08-01-0.json.gz` .. `-23.json.gz`) |
| Tổng dung lượng nén | ~480 MB |
| Tổng số event | **4,071,556** |
| `created_at` min/max | `2026-08-01T00:00:00Z` → `2026-08-01T23:59:58Z` (đúng 1 ngày, không thiếu giờ) |
| `_corrupt_record` | không có cột này → **không có dòng JSON nào bị lỗi parse** |
| `id` trùng lặp | 0 |

## 2. Top-level schema (8 field, ổn định — khớp `schema_notes.md`)

```
actor, created_at, id, org, payload, public, repo, type
```

- `public`: **không phải luôn `true`** như có thể ngộ nhận — 6/4,071,556 event
  có `public = false`. Số lượng nhỏ nhưng nếu Silver/Gold giả định
  `WHERE public = true` là không cần lọc thì đây là 6 dòng lọt lưới âm thầm.
- `org` IS NULL: 3,913,007 / 4,071,556 (**96.1%**) — phần lớn event không thuộc
  repo tổ chức. Nếu Silver có join/lookup theo `org.login`, 96% dòng sẽ null ở
  cột đó — đừng thiết kế transform giả định `org` luôn có giá trị.

## 3. Phân bố event type (cả ngày, 16 loại — không phải 12 như mẫu 1 giờ)

| type | count | % |
|---|---:|---:|
| PushEvent | 3,894,278 | 95.65% |
| CreateEvent | 118,162 | 2.90% |
| DeleteEvent | 49,913 | 1.23% |
| PullRequestEvent | 3,891 | 0.10% |
| IssueCommentEvent | 1,545 | 0.04% |
| IssuesEvent | 1,164 | 0.03% |
| PullRequestReviewCommentEvent | 736 | 0.02% |
| WatchEvent | 682 | 0.02% |
| PullRequestReviewEvent | 650 | 0.02% |
| ReleaseEvent | 257 | <0.01% |
| ForkEvent | 172 | <0.01% |
| MemberEvent | 42 | <0.01% |
| CommitCommentEvent | 25 | <0.01% |
| PublicEvent | 17 | <0.01% |
| GollumEvent | 14 | <0.01% |
| DiscussionEvent | 8 | <0.01% |

`GollumEvent`, `DiscussionEvent`, `PublicEvent`, `CommitCommentEvent` **không
xuất hiện** trong sample hour của `schema_notes.md` — chỉ lộ ra khi quét đủ 24
giờ. Bài học: 1 file mẫu không đủ để liệt kê hết các `type`; bất kỳ code nào
hardcode danh sách event type (vd. Step 4 split-by-type) nên đọc lại danh sách
này thay vì danh sách 12 loại trong `schema_notes.md`.

`PushEvent` chiếm 95.65% — khớp nhận định "PushEvent dominates" của Step 2,
nhưng con số thật (95.65%, không phải ~97%) nên dùng số này khi viết luận điểm
trong docs/portfolio sau này.

## 4. `payload` union schema — ĐÍNH CHÍNH quan trọng so với Step 2

`schema_notes.md` viết: *"Spark's inferred schema for payload ends up
containing every field from every event type combined — **several hundred
nested fields total**"*.

Thực tế trên dữ liệu đầy đủ: **`payload` chỉ có 25 field top-level**, không
phải "hàng trăm":

```
action, assignee, assignees, before, comment, description, discussion,
forkee, full_ref, head, issue, label, labels, master_branch, member,
number, pages, pull_request, push_id, pusher_type, ref, ref_type,
release, repository_id, review
```

("Hàng trăm" có thể đúng nếu đếm *toàn bộ field ở mọi cấp lồng nhau* — vd.
bên trong `payload.pull_request.head.repo.owner...` — nhưng đó là điểm khác
với "field trong payload". Cần diễn đạt lại claim này cho chính xác trước khi
đưa vào portfolio.)

### 4.1. Phát hiện quan trọng nhất: `PushEvent.payload` KHÔNG có `commits`

`schema_notes.md` và kế hoạch Step 4 đều giả định:

> `payload.commits` (array, PushEvent only) → exploded to one row per commit

Nhưng field `commits`, `size`, `distinct_size` **không tồn tại** trong union
schema của bộ dữ liệu này. Field còn lại thực sự dùng được cho `PushEvent`:

| field | non-null (trên 3,894,278 PushEvent) |
|---|---:|
| `payload.push_id` | 100.0% |
| `payload.ref` | 100.0% |
| `payload.before` | 100.0% |
| `payload.head` | 100.0% |
| `payload.repository_id` | 100.0% |

**Tác động trực tiếp lên Step 4 (Silver)**: kế hoạch "explode commits" trong
`schema_notes.md` **không chạy được as-is** trên dữ liệu này — không có mảng
commit nào để explode.

**Quyết định (đã chốt):** dùng `payload.labels` trên `PullRequestEvent` làm
ví dụ `explode()` chính cho Step 4, thay cho `PushEvent.commits`. Khảo sát
toàn bộ union schema chỉ tìm thấy 3 field array top-level trong `payload`:
`assignees`, `labels`, `pages`. So sánh (toàn bộ 24 file):

| field | event type | events có data | avg size | max size |
|---|---|---:|---:|---:|
| `payload.pages` | GollumEvent | 14 | 8.2 | 74 |
| `payload.assignees` | IssuesEvent/PullRequestEvent | 64/52 | ~1.1 | 2-3 |
| **`payload.labels`** | **PullRequestEvent** | **1,336** | **2.37** | **43** |
| `payload.labels` | IssuesEvent | 530 | 3.13 | 19 |

`PullRequestEvent.payload.labels` thắng vì volume thật (1,336 event có data,
so với `pages` chỉ 14 — quá hiếm để làm ví dụ chính) và phân bố đẹp cho việc
dạy explode (trung bình 2.37 label/PR nhưng có PR tới 43 label — vừa minh
hoạ rõ "1 dòng → N dòng", vừa là ví dụ thật cho **data skew** ở Step 6).

Cả hai field `size`/`distinct_size` cũng biến mất — nếu Gold (Step 5) định
tính "tổng số commit mỗi push" bằng `sum(payload.size)`, số liệu đó sẽ toàn
NULL. Cần quyết định hướng đi này TRƯỚC khi viết code Step 4/5, không phát
hiện giữa chừng.

## 5. Data skew (actor & repo) — số liệu thật cho Step 5/6

| | cardinality |
|---|---:|
| distinct `actor.login` | 305,494 |
| distinct `repo.name` | 478,353 |

Top 10 actor theo số event:

| actor.login | count |
|---|---:|
| github-actions[bot] | 227,280 |
| dependabot[bot] | 23,248 |
| renovate[bot] | 11,764 |
| ugmoddev | 7,906 |
| zerotraceh1 | 7,609 |
| swa-runner-app[bot] | 5,941 |
| OpenHarmonySCM-noreply | 5,746 |
| r00tsh00t12345 | 5,726 |
| pull[bot] | 4,240 |
| 2000pd3rvr | 4,235 |

`github-actions[bot]` một mình chiếm **5.58%** tổng số event toàn ngày (CI bot
auto-commit) — đây là ví dụ skew thật, không phải lý thuyết, để dùng khi dạy
`groupBy("actor_login")` ở Step 5/6: 1 key duy nhất có thể nhận > 200K dòng
trong khi phần lớn actor khác dưới 10. Khi `groupBy` shuffle theo actor, 1
partition sẽ nặng hơn hẳn — ví dụ cụ thể để minh hoạ `salting` / `AQE skew
join` ở Step 6 thay vì chỉ nói suông "long tail".

Top repo cũng skew tương tự (`zerotraceh1/er-forge-probe`: 7,609 event) nhưng
ít cực đoan hơn actor — đáng chú ý các top repo đều là tên lạ dạng
probe/scanner (`er-forge-probe`, `email-probe`) — nên kiểm tra xem đây có phải
traffic bot/scanner tự động trước khi coi là "repo phổ biến" trong bất kỳ demo
Gold nào về "top active repos".

## 6. `created_at` — vẫn cần cast (không đổi so với Step 2)

Vẫn là string ISO-8601, range đúng 1 ngày liên tục, không có giá trị dị
thường ngoài `2026-08-01`. Xác nhận lại: cast sang `timestamp` ở Silver là an
toàn, không cần xử lý timezone lạ hay giá trị null.

## 7. Ghi chú vận hành trên Windows (gặp phải khi chạy script này)

Không liên quan trực tiếp đến dữ liệu nhưng đáng lưu lại vì sẽ tái diễn ở
Step 3+ khi code thật sự ghi/đọc Parquet trên nhiều file:

- `spark.read.json("data/raw/*.json.gz")` (wildcard) **crash** trên Windows vì
  Hadoop's `globStatus()` gọi `NativeIO.Windows.access0` — cần `winutils.exe`
  (`HADOOP_HOME`) mà project chưa cài. Workaround: liệt kê file tường minh bằng
  `Path.glob()` phía Python rồi truyền **list** path vào `spark.read.json([...])`
  thay vì truyền 1 string wildcard.
- `.cache()` một DataFrame còn nguyên union schema (25 field payload lồng
  nhau) trên 4 triệu dòng làm JVM driver 2GB (cấu hình mặc định trong
  `spark/session.py`) chết kết nối Py4J (OOM) mà không báo lỗi rõ ràng
  (`Py4JJavaError: <exception str() failed>`). Cách né: chỉ `select()` các cột
  thật sự cần rồi mới `.cache()` phần hẹp đó, không cache cả DataFrame gốc khi
  làm việc trên quy mô nhiều-triệu-dòng.
- Print tiếng Việt có dấu khi output bị redirect ra file trên Windows cần
  `sys.stdout.reconfigure(encoding="utf-8")` — pattern này project đã áp dụng
  ở `spark/session.py` và `ingestion/download_gharchive.py`, nhưng
  `spark/explore_schema.py` (Step 2) **thiếu dòng này** — sẽ crash nếu ai đó
  redirect output của nó ra file thay vì xem trực tiếp trên console.

## 8. Khuyến nghị cho Step 3+ (tóm tắt hành động)

1. **Step 4 explode**: dùng `PullRequestEvent.payload.labels` (đã chốt ở mục
   4.1), không dùng `PushEvent.payload.commits` (không tồn tại).
2. Silver nên tách theo `type` như dự định, nhưng lấy danh sách 16 type ở
   mục 3, không phải 12 type cũ.
3. Bronze/Silver không nên giả định `org` luôn có giá trị (96% null) hay
   `public` luôn `true` (6 dòng false) — filter tường minh thay vì implicit.
4. Step 5/6 (aggregation, skew): dùng `github-actions[bot]` /
   `zerotraceh1/er-forge-probe` làm ví dụ thật cho skew, không cần dữ liệu
   giả lập. `payload.labels` size (avg 2.37, max 43 trên PullRequestEvent)
   cũng là ví dụ skew thật ở tầng payload, dùng thêm được nếu cần.
5. Nếu Step 3+ cần đọc nhiều file cùng lúc trên Windows, dùng pattern liệt kê
   file tường minh ở mục 7, tránh lặp lại lỗi glob.
