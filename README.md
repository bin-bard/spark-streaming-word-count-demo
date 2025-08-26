# Spark Streaming Word Count Demo

Demo ứng dụng đơn giản sử dụng Spark Streaming (DStreams) để đếm tần suất các từ trong luồng dữ liệu văn bản thời gian thực. Dự án này cung cấp hai phiên bản:

1. `word_count.py`: Đếm tất cả các từ (cho Docker)
2. `word_count_windows.py`: Đếm tất cả các từ (cho Windows)

## Yêu cầu

### Cho Windows Native:

- Windows 10/11
- Java 21
- Python 3.11
- Apache Spark
- Netcat cho Windows
- Hadoop winutils

### Cho Docker:

- Windows 10/11
- Docker Desktop for Windows (bao gồm Docker Compose)
- Terminal/Command Prompt hoặc PowerShell

## Hướng dẫn thực thi

## Phần 1: Chạy trên Windows Native

### Bước 1: Kiểm tra Java

Nếu đã cài đặt Java 21, hãy kiểm tra biến môi trường:

```
java -version
```

Đảm bảo biến môi trường JAVA_HOME đã được thiết lập:

```
JAVA_HOME=C:\Program Files\Java\jdk-21
PATH=%JAVA_HOME%\bin;%PATH%
```

### Bước 2: Cài đặt Apache Spark

1. Tải Apache Spark từ [https://spark.apache.org/downloads.html](https://spark.apache.org/downloads.html)

   **Lưu ý quan trọng**: Chọn đúng phiên bản prebuilt để phù hợp với winutils:

   - **Spark prebuilt for Apache Hadoop 2.7**: Tương thích với winutils `hadoop-2.7.1`
   - **Spark prebuilt for Apache Hadoop 3.2 and later**: Tương thích với winutils `hadoop-3.0.0` trở lên

2. Giải nén vào `C:\spark`
3. Thiết lập biến môi trường:

```
SPARK_HOME=C:\spark
PATH=%SPARK_HOME%\bin;%PATH%
```

### Bước 3: Cài đặt Hadoop winutils

Cài đặt winutils phù hợp với phiên bản Spark prebuilt đã chọn ở Bước 2:

1. Tải `winutils.exe` từ [https://github.com/steveloughran/winutils](https://github.com/steveloughran/winutils)

   - Chọn thư mục phiên bản Hadoop tương ứng với Spark prebuilt
   - Tải file `winutils.exe` từ thư mục `bin/`

2. Tạo thư mục `C:\hadoop\bin`
3. Copy `winutils.exe` vào `C:\hadoop\bin`
4. Thiết lập biến môi trường:

```
HADOOP_HOME=C:\hadoop
PATH=%HADOOP_HOME%\bin;%PATH%
```

**Lưu ý**: Sử dụng đúng phiên bản winutils để tránh lỗi khi chạy Spark trên Windows.

### Bước 4: Cài đặt Netcat

Sử dụng ncat (từ Nmap):

1. Tải Nmap từ [https://nmap.org/download.html](https://nmap.org/download.html)
2. Cài đặt Nmap (bao gồm ncat)

### Bước 5: Cài đặt Python dependencies

Nếu đã có Python 3.11, chỉ cần cài đặt PySpark:

```
pip install pyspark==3.3.0
```

### Bước 6: Chạy ứng dụng

#### Terminal 1: Khởi động Netcat

```
ncat -lk 9999
```

#### Terminal 2: Chạy Spark application

```
cd spark-streaming-word-count-demo/Windows
spark-submit word_count_windows.py
```

### Bước 7: Kiểm tra kết quả

Quay lại terminal netcat (Terminal 1), gõ một vài câu và nhấn Enter:

```
hello spark hello world
spark streaming is fun
hello world again
```

Quan sát terminal spark-submit (Terminal 2). Sau mỗi 2 giây (batch interval), kết quả đếm từ sẽ được in ra màn hình.

### Dọn dẹp Windows

Nhấn Ctrl+C để dừng cả hai terminal.

---

## Phần 2: Chạy với Docker

### Bước 1: Khởi động cụm Spark

Mở terminal, điều hướng đến thư mục `Docker` trong dự án và chạy:

```
cd Docker
docker-compose up -d
```

### Bước 2: Chạy ứng dụng Spark

Mở một terminal mới và chạy lệnh sau:

```
docker-compose exec spark-master spark-submit /app/word_count.py
```

### Bước 3: Gửi dữ liệu qua Netcat

Mở một terminal khác và kết nối tới netcat server:

```
docker attach netcat-server
```

Bây giờ có thể gõ dữ liệu trực tiếp để gửi tới Spark streaming:

```
hello spark hello world
spark streaming is fun
hello world again
```

### Bước 4: Kiểm tra kết quả

Quan sát terminal spark-submit (Bước 2). Sau mỗi 2 giây (batch interval), kết quả đếm từ sẽ được in ra màn hình.

**Lưu ý:** Để thoát khỏi netcat server, nhấn `Ctrl+P` sau đó `Ctrl+Q`.

### Dọn dẹp

Khi hoàn thành demo, nhấn Ctrl+C để dừng docker-compose, sau đó chạy:

```
docker-compose down
```
