Bước 1: docker-compose up -d
-> Sau khi chạy sẽ báo cáo: webserver bị lỗi -> Sau đó scheduler bị lỗi luôn

Bước 2: VÀo docker desktop -> Vào webserver, kiểm tra chạy xong chưa: Listening at: http://10.200.2.51:8080 (45)

Bước 3: Sau khi thành công, tiếp tục chạy lại: docker-compose up -d (Để chạy scheduler)

Bước 4: Vào docker desktop -> Kiểm tra scheduler -> Nó phải chạy một thời gian, vì download packages các kiểu.
Chạy thành công: Listening at: http://10.200.2.51:8793 (72)

Bước 5: Chạy thành công
9021: control center của Kafka Confluent
8080: Apache Airflow 
MK: admin
Password: admin

Bước 6: Chạy lệnh khởi động Spark
docker exec -it spark-master spark-submit  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.7.4  /opt/spark_app/spark_stream.py

-> Hoàn thành chu ETL Datastreaming -> Bây giờ mỗi lần mình ấn Trigger DAGs ở Apache Airflow -> Nó tự động xử lý.

Bước 7: Kiểm tra xem dữ liệu có được streaming vào PostgresSQL hay không:
docker exec -it postgres psql -U airflow -d airflow

Hiển thị các hàng có trong CSDL:
SELECT COUNT(*) FROM processed_data;


Chỉ làm đến Kafka thôi: https://www.youtube.com/watch?v=GqAcTrqKcrY&t=3107s
đoạn sau setup PostgreSQL và Spark: https://medium.com/@dhirajmishra57/streaming-data-using-kafka-postgresql-spark-streaming-airflow-and-docker-33c43bfa609d

tham khảo thêm cách setup Spark & Cassandra (để làm đối chứng): https://github.com/airscholar/e2e-data-engineering/blob/main/spark_stream.py

