- docker network create confluent
docker-compose -f docker-compose-minio.yml up

- Tại localhost:9001
username: minioadmin
password: minioadmin

- Tại Minio, tạo key (access_key, secret_key), thay vào minio_config.py 

- python load_to_storage.py
Vào docker desktop personal, chờ cho webserver chạy xong.
docker-compose up -d

Chờ cho schduler chạy xong -> Hoàn tất setup Airflow

- Xong scheduluer là xong tất cả
http://localhost:9001/ -> MinIIO 
http://localhost:9021/ -> Kafka
http://localhost:8080/ -> Airflow, username = admin, pass = admin

- Setup Lambda ở MinIO
MinIO/Administrator/Events -> Add an Event destination, chọn Kafka
Identifier: thedeptrai
Brokers: broker:29092
Topic: training_data_streaming

- Save event destination -> Restart
Sử dụng Lambda cho Bucket:
MinIO/Administrator/Buckets, chọn bronze, ấn vào Events -> Subscribe to Event, ARN (Cái vừa tạo), Select Event chỉ chọn Put - Object Uploaded

- Vào Apache Airflow -> Chọn data_streaming -> Trigger DAG
minio_config.py
