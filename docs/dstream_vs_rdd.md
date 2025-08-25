# So sánh DStream vs RDD

## 🎯 Tổng quan

**DStream** (Discretized Stream) và **RDD** (Resilient Distributed Dataset) là hai khái niệm cốt lõi trong Apache Spark, nhưng phục vụ các mục đích khác nhau.

## 📊 Bảng so sánh chi tiết

| Đặc điểm | DStream | RDD |
|----------|---------|-----|
| **Định nghĩa** | Chuỗi liên tục các RDD | Distributed dataset tĩnh |
| **Loại dữ liệu** | Streaming/Real-time | Batch/Historical |
| **Cấu trúc bên trong** | Sequence of RDDs over time | Single distributed collection |
| **Tính bất biến** | ✅ Immutable | ✅ Immutable |
| **Fault Tolerance** | ✅ Yes (qua lineage) | ✅ Yes (qua lineage) |
| **Lazy Evaluation** | ✅ Yes | ✅ Yes |
| **Caching** | ✅ Yes | ✅ Yes |

## 🔄 Cấu trúc dữ liệu

### RDD - Static Dataset
```python
# RDD: Dataset tĩnh tại một thời điểm
rdd = sc.parallelize([1, 2, 3, 4, 5])
# rdd chứa: [1, 2, 3, 4, 5]
```

### DStream - Dynamic Dataset
```python
# DStream: Chuỗi các RDD theo thời gian
lines = ssc.socketTextStream("localhost", 9999)

# T=0s: lines → RDD["hello world"]
# T=2s: lines → RDD["spark streaming"] 
# T=4s: lines → RDD["real time data"]
```

## ⚡ Operations (Phép toán)

### RDD Operations
```python
# Transformations (lazy)
rdd.map(lambda x: x * 2)
rdd.filter(lambda x: x > 3)
rdd.flatMap(lambda x: [x, x*2])

# Actions (eager)  
rdd.collect()
rdd.count()
rdd.take(5)
```

### DStream Operations
```python
# Transformations (lazy)
dstream.map(lambda x: x.upper())
dstream.filter(lambda x: len(x) > 4)
dstream.flatMap(lambda line: line.split())

# Output Operations (eager - trigger execution)
dstream.pprint()
dstream.saveAsTextFiles("output")
dstream.foreachRDD(lambda rdd: rdd.collect())
```

## 🔧 Ví dụ thực tế

### Xử lý với RDD (Batch)
```python
# Đọc file tĩnh
lines_rdd = sc.textFile("input.txt")

# Word count trên toàn bộ file
words_rdd = lines_rdd.flatMap(lambda line: line.split(" "))
word_counts_rdd = words_rdd.map(lambda word: (word, 1)) \
                           .reduceByKey(lambda x, y: x + y)

# Lấy kết quả một lần
results = word_counts_rdd.collect()
print(results)  # [(word1, count1), (word2, count2), ...]
```

### Xử lý với DStream (Streaming)
```python
# Đọc stream liên tục
lines_dstream = ssc.socketTextStream("localhost", 9999)

# Word count cho mỗi batch
words_dstream = lines_dstream.flatMap(lambda line: line.split(" "))
word_counts_dstream = words_dstream.map(lambda word: (word, 1)) \
                                   .reduceByKey(lambda x, y: x + y)

# In kết quả liên tục
word_counts_dstream.pprint()

# Bắt đầu streaming
ssc.start()
ssc.awaitTermination()
```

## 🎭 Lifecycle (Vòng đời)

### RDD Lifecycle
```
1. Create RDD → 2. Transform → 3. Action → 4. Result
    (một lần)        (lazy)       (eager)     (kết thúc)
```

### DStream Lifecycle  
```
1. Create DStream → 2. Transform → 3. Output Op → 4. Repeat...
    (setup)           (lazy)        (eager)       (liên tục)
```

## 🧪 Code Demo: RDD vs DStream

### RDD Example
```python
from pyspark import SparkContext

sc = SparkContext("local", "RDDExample")

# Tạo RDD từ dữ liệu có sẵn
data = ["hello world", "spark is awesome", "rdd processing"]
lines_rdd = sc.parallelize(data)

# Xử lý word count
words_rdd = lines_rdd.flatMap(lambda line: line.split(" "))
word_counts_rdd = words_rdd.map(lambda word: (word, 1)) \
                           .reduceByKey(lambda x, y: x + y)

# Lấy kết quả
print("RDD Results:")
for word, count in word_counts_rdd.collect():
    print(f"{word}: {count}")

sc.stop()
```

### DStream Example
```python
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]", "DStreamExample")  
ssc = StreamingContext(sc, 2)

# Tạo DStream từ socket
lines_dstream = ssc.socketTextStream("localhost", 9999)

# Xử lý word count liên tục
words_dstream = lines_dstream.flatMap(lambda line: line.split(" "))
word_counts_dstream = words_dstream.map(lambda word: (word, 1)) \
                                   .reduceByKey(lambda x, y: x + y)

# In kết quả liên tục
word_counts_dstream.pprint()

print("DStream Results (continuous):")
ssc.start()
ssc.awaitTermination()
```

## 🎯 Khi nào dùng gì?

### Sử dụng RDD khi:
- ✅ Xử lý dữ liệu batch/historical
- ✅ Dữ liệu đã có sẵn trong files/databases  
- ✅ Phân tích one-time, ad-hoc queries
- ✅ Machine learning trên static dataset

### Sử dụng DStream khi:
- ✅ Xử lý dữ liệu real-time/streaming
- ✅ Dữ liệu đến liên tục (logs, sensors, events)
- ✅ Cần kết quả ngay lập tức
- ✅ Monitoring, alerting systems

## 🔗 Mối quan hệ

```python
# DStream BÊN TRONG chứa các RDD
dstream = ssc.socketTextStream("localhost", 9999)

# Mỗi batch interval, DStream tạo ra một RDD mới
def process_rdd(time, rdd):
    print(f"Processing RDD at time {time}")
    print(f"RDD has {rdd.count()} elements")
    
    # Có thể xử lý RDD như bình thường
    results = rdd.collect()
    print(f"Results: {results}")

# Truy cập RDD bên trong DStream
dstream.foreachRDD(process_rdd)
```

## 💡 Điểm mấu chốt

1. **DStream = Chuỗi các RDD theo thời gian**
2. **RDD = Snapshot dữ liệu tại một thời điểm**  
3. **DStream dùng cho real-time, RDD dùng cho batch**
4. **Cả hai đều immutable và fault-tolerant**
5. **DStream operations áp dụng lên mỗi RDD bên trong**

---

🎓 **Kết luận**: DStream là abstraction cao hơn cho streaming, nhưng bên trong vẫn sử dụng RDD để xử lý từng micro-batch.