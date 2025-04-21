1. Setup for development stage
- Setting up MLFlow: https://www.youtube.com/watch?v=ybSLoRsZCKI&list=PLVVBQldz3m5sxb8uBZUlqoPRXZoxzz_7U&index=9
    ```bash
        conda activate kafka-env
    ```
    ```bash
        pip install mlflow
    ```
- Để khắc phục sự không tương thích giữa jsonschema và jupyterlab-server, bạn có thể thử nâng cấp jsonschema lên phiên bản mới hơn (>=4.17.3) bằng cách chạy lệnh:
    ```bash
    pip install jsonschema>=4.17.3
    ```
- run mlflow: 
    ```bash
    mlflow server --backend-store-uri ./mlruns --host 0.0.0.0 --port 5000
    mlflow server --backend-store-uri sqlite:///mlflow.db --default-artifact-root ./mlruns --host 0.0.0.0 --port 5000
    ```
- Link: http://10.200.2.51:5000/#/experiments/0
- Sử dụng SQLite làm backend store. Dữ liệu của MLflow (chẳng hạn như thông tin về các experiments, runs, parameters, metrics) sẽ được lưu trong tệp mlflow.db.
```bash
    mlflow server --host 10.200.2.51 --port 5000
```
- create a ML model with mlflow tracker:
    mlflow.set_tracking_uri('http://10.200.2.51:5000')
    mlflow.set_experiment('exp-1')

2; Các cổng được sử dụng trong project:
- Streaming: 
    - Kafka: 9002, 29092
    - Zookeeper: 2181
    - Schema registry: 8081
    - Kafka control center: 9021
    - Redis: 6379
- Storage:
    - MinIO: 9000 - API server, 9001
    - PostgreSQL: 5432
- Processing:
    - Spark master: 7077
    - Ray: 6379 - main port, 8265 - dashboard, 10001 - client server
- Monitor: 
    - Airflow: 8080 (with username/password: admin/admin)
    - Prometheus: 9090
    - Grafana: 3000 (internal), 3009 (iframe access)
- Model registry & serving:
    - MLFlow: 5000