"""Tạo SparkSession chạy local — điểm khởi đầu của mọi script Spark trong dự án.

Chạy trực tiếp file này (`python spark/session.py`) để xem SparkSession khởi
tạo thành công và lấy URL của Spark UI.
"""

import sys
import time

from pyspark.sql import SparkSession

# Box-drawing / ký tự đặc biệt trong log của Spark hoặc output .show() có thể
# crash trên console Windows còn dùng codepage cp1252 mặc định — ép UTF-8 để
# tránh lỗi này (cùng pattern với ingestion/download_gharchive.py).
sys.stdout.reconfigure(encoding="utf-8")


def get_spark_session(app_name: str = "gh-archive-pyspark") -> SparkSession:
    """Tạo (hoặc lấy lại) một SparkSession chạy local.

    Gọi hàm này KHÔNG chạy bất kỳ job Spark nào — nó chỉ khởi động JVM,
    SparkContext, và các dịch vụ nền (Spark UI, scheduler...). Dữ liệu chỉ
    thực sự được đọc/xử lý khi một "action" (vd. .show(), .count(),
    .write.parquet()) được gọi trên một DataFrame sau này.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        # "local[*]": chạy Spark trên chính máy này (không phải cluster thật),
        # dùng tất cả CPU core sẵn có làm "worker". Đây là lý do ta học được
        # Spark mà không cần Databricks/cluster — engine y hệt, chỉ khác nơi chạy.
        .master("local[*]")
        # spark.sql.shuffle.partitions: số lượng partition Spark tạo ra MỖI
        # KHI có shuffle (vd. sau groupBy, join, orderBy). Mặc định là 200 —
        # con số này được thiết kế cho cluster nhiều máy, dữ liệu lớn. Với
        # dữ liệu demo nhỏ chạy trên 1 máy, 200 partition nhỏ xíu chỉ tạo ra
        # overhead (200 task nhỏ, mỗi task tốn thời gian khởi động > thời gian
        # xử lý thực sự). Ta hạ xuống 8 để phù hợp với quy mô demo local.
        .config("spark.sql.shuffle.partitions", "8")
        # Giới hạn RAM cho JVM driver (nơi chạy code Python driver + thu thập
        # kết quả .collect()/.show()). 2g là đủ cho dữ liệu demo vài ngày
        # GH Archive; sẽ cần tăng nếu xử lý dữ liệu nhiều tháng/năm.
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

    # sparkContext.uiWebUrl chỉ có giá trị SAU khi SparkContext đã khởi tạo
    # xong (tức sau getOrCreate() ở trên) — đây không phải action trên dữ
    # liệu, chỉ là thông tin về chính Spark engine.
    print(f"[spark] SparkSession sẵn sàng — appName={app_name}")
    print(f"[spark] Spark UI: {spark.sparkContext.uiWebUrl}")

    return spark


if __name__ == "__main__":
    spark = get_spark_session()

    # Giữ tiến trình sống vài giây để bạn kịp mở Spark UI trên trình duyệt
    # (http://localhost:4040 mặc định) — UI sẽ tắt ngay khi SparkSession dừng.
    print("[spark] Giữ session 30 giây để bạn mở Spark UI... (Ctrl+C để thoát sớm)")
    try:
        time.sleep(30)
    except KeyboardInterrupt:
        # Trên Windows, Ctrl+C gửi CTRL_C_EVENT cho CẢ tiến trình JVM con mà
        # Py4J khởi chạy ngầm, không chỉ tiến trình Python này — nên JVM có
        # thể đã chết trước khi dòng spark.stop() bên dưới kịp chạy. Bọc
        # try/except quanh spark.stop() để tránh traceback gây hiểu lầm là
        # lỗi, dù JVM đã tắt hay chưa.
        pass

    try:
        spark.stop()
        print("[spark] Đã dừng SparkSession.")
    except Exception:
        print("[spark] SparkSession/JVM đã tắt trước đó (bình thường khi ngắt bằng Ctrl+C trên Windows).")
