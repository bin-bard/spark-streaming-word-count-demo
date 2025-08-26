# Spark Streaming Word Count Demo

Demo ứng dụng đơn giản sử dụng Spark Streaming (DStreams) để đếm tần suất các từ trong luồng dữ liệu văn bản thời gian thực. Dự án này cung cấp hai phiên bản:
1. `word_count_streaming.py`: Đếm tất cả các từ
2. `word_count_filter.py`: Chỉ đếm các từ có độ dài lớn hơn 4 ký tự

## Yêu cầu

- Windows
- Docker Desktop for Windows
- Visual Studio Code (hoặc IDE khác)

## Hướng dẫn thực thi

### Bước 1: Khởi động cụm Spark

Mở terminal và điều hướng đến thư mục dự án, sau đó chạy:

```
docker-compose up
```

### Bước 2: Tìm tên network của Docker

Mở một terminal mới và chạy:

```
docker network ls
```

Tìm tên network có dạng `spark-streaming-word-count-demo_default` hoặc tương tự.

### Bước 3: Khởi động Netcat để gửi dữ liệu

Trong terminal mới, chạy lệnh sau (thay `[YOUR_NETWORK_NAME]` bằng tên network vừa tìm được):

```
docker run -it --rm --network=[YOUR_NETWORK_NAME] alpine nc -lk -p 9999
```

Ví dụ: `docker run -it --rm --network=spark-streaming-word-count-demo_default alpine nc -lk -p 9999`

### Bước 4: Chạy ứng dụng Spark

Mở một terminal khác và chạy một trong hai lệnh sau:

#### Để đếm tất cả các từ:
```
docker exec spark-master /opt/bitnami/spark/bin/spark-submit /app/word_count_streaming.py
```

#### Để chỉ đếm các từ có độ dài lớn hơn 4 ký tự:
```
docker exec spark-master /opt/bitnami/spark/bin/spark-submit /app/word_count_filter.py
```

### Bước 5: Kiểm tra kết quả

Quay lại terminal netcat (Bước 3), gõ một vài câu và nhấn Enter:

```
hello spark hello world
spark streaming is fun
hello world again
```

Quan sát terminal spark-submit (Bước 4). Sau mỗi vài giây (batch interval), bạn sẽ thấy kết quả đếm từ được in ra màn hình. Tùy vào file Python bạn chọn chạy, kết quả sẽ hiển thị tất cả các từ hoặc chỉ các từ có độ dài lớn hơn 4 ký tự.

### Dọn dẹp

Khi hoàn thành demo, nhấn Ctrl+C để dừng docker-compose, sau đó chạy:

```
docker-compose down
```
