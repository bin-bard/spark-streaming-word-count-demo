# Kiến trúc Spark Streaming

## 🏗️ Tổng quan kiến trúc

Spark Streaming sử dụng mô hình **Micro-batching** để xử lý dữ liệu thời gian thực:

```
Input Data Stream → Micro-batches → Spark Engine → Output
```

## 📊 Mô hình Micro-batching

### 1. **Input Layer - Lớp nhập dữ liệu**
```
Continuous Data Stream
    ↓
┌─────────┬─────────┬─────────┬─────────┐
│ Batch 1 │ Batch 2 │ Batch 3 │ Batch 4 │ ...
│  (2s)   │  (2s)   │  (2s)   │  (2s)   │
└─────────┴─────────┴─────────┴─────────┘
```

### 2. **Processing Layer - Lớp xử lý**
Mỗi micro-batch được xử lý như một **RDD** thông thường:

```python
# Mỗi 2 giây, Spark tạo một RDD từ data stream
lines = ssc.socketTextStream("localhost", 9999)  # DStream
# lines tại thời điểm T1 → RDD[String]
# lines tại thời điểm T2 → RDD[String]  
# lines tại thời điểm T3 → RDD[String]
```

### 3. **Output Layer - Lớp xuất dữ liệu**
Kết quả được ghi ra external systems hoặc console.

## 🔄 Luồng xử lý chi tiết

### Ví dụ với Word Count:

```python
# 1. Tạo DStream từ socket
lines = ssc.socketTextStream("localhost", 9999)

# 2. Áp dụng transformations (lazy evaluation)
words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
counts = pairs.reduceByKey(lambda x, y: x + y)

# 3. Output operation (trigger execution)
counts.pprint()
```

### Thực thi:
```
T=0s:  Input: "hello world"
       → RDD: ["hello", "world"] 
       → Output: (hello,1), (world,1)

T=2s:  Input: "hello spark"  
       → RDD: ["hello", "spark"]
       → Output: (hello,1), (spark,1)

T=4s:  Input: "world spark"
       → RDD: ["world", "spark"] 
       → Output: (world,1), (spark,1)
```

## ⚡ Ưu điểm của Micro-batching

### 1. **Fault Tolerance**
- Mỗi batch có thể được recompute nếu fail
- Lineage tracking như RDD thông thường
- Exactly-once semantics

### 2. **Scalability** 
- Sử dụng toàn bộ cluster của Spark
- Load balancing tự động
- Dynamic resource allocation

### 3. **Integration**
- Dùng chung API với Spark Core và SQL
- Có thể kết hợp với MLlib, GraphX
- Consistent programming model

## 🎯 So sánh với Stream Processing thuần túy

| Đặc điểm | Micro-batching (Spark) | Pure Streaming (Storm, Flink) |
|----------|------------------------|--------------------------------|
| **Latency** | Cao hơn (seconds) | Thấp hơn (milliseconds) |
| **Throughput** | Rất cao | Trung bình |
| **Fault Tolerance** | Excellent | Good |
| **Exactly-once** | Dễ dàng | Phức tạp |
| **Learning Curve** | Dễ (như batch) | Khó hơn |

## 🔧 Các thành phần chính

### 1. **StreamingContext**
```python
ssc = StreamingContext(sc, batch_duration)
# batch_duration: thời gian của mỗi micro-batch
```

### 2. **DStream (Discretized Stream)**
- Abstraction của continuous data stream
- Internally: sequence of RDDs
- Immutable và distributed

### 3. **Transformations**
```python
# Stateless transformations
map(), filter(), flatMap(), union()

# Stateful transformations  
reduceByKey(), groupByKey(), join()

# Windowing transformations
window(), reduceByWindow()
```

### 4. **Output Operations**
```python
print()        # Console output
saveAsTextFiles()  # File output
foreachRDD()   # Custom output
```

## 📈 Performance Tuning

### 1. **Batch Duration**
```python
# Quá ngắn: overhead cao
ssc = StreamingContext(sc, 0.5)  # 500ms

# Quá dài: latency cao  
ssc = StreamingContext(sc, 10)   # 10s

# Tối ưu: cân bằng throughput vs latency
ssc = StreamingContext(sc, 2)    # 2s (thường dùng)
```

### 2. **Parallelism**
```python
# Tăng parallelism cho input
lines = ssc.socketTextStream("localhost", 9999)
lines = lines.repartition(4)  # Chia thành 4 partitions

# Hoặc union multiple streams
stream1 = ssc.socketTextStream("host1", 9999) 
stream2 = ssc.socketTextStream("host2", 9999)
combined = stream1.union(stream2)
```

### 3. **Memory Management**
```python
# Thiết lập storage level
lines.persist(StorageLevel.MEMORY_AND_DISK_SER)

# Checkpoint cho stateful operations
ssc.checkpoint("hdfs://checkpoint")
```

## 🔍 Monitoring & Debugging

### 1. **Spark UI**
- Streaming tab: batch processing times
- Jobs tab: detailed execution plan
- Storage tab: cached RDDs

### 2. **Logs**
```python
sc.setLogLevel("WARN")  # Giảm log noise
```

### 3. **Metrics**
```python
# Custom metrics
def count_records(time, rdd):
    count = rdd.count()
    print(f"Processed {count} records at {time}")

dstream.foreachRDD(count_records)
```

---

📚 **Tham khảo thêm**: [Spark Streaming Programming Guide](https://spark.apache.org/docs/latest/streaming-programming-guide.html)