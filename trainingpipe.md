Setting up MLFlow: https://www.youtube.com/watch?v=ybSLoRsZCKI&list=PLVVBQldz3m5sxb8uBZUlqoPRXZoxzz_7U&index=9

conda activate kafka-env

pip install mlflow

Để khắc phục sự không tương thích giữa jsonschema và jupyterlab-server, bạn có thể thử nâng cấp jsonschema lên phiên bản mới hơn (>=4.17.3) bằng cách chạy lệnh:
    pip install jsonschema>=4.17.3


run mlflow: 
    mlflow server --backend-store-uri ./mlruns --host 0.0.0.0 --port 5000
    mlflow server --backend-store-uri sqlite:///mlflow.db --default-artifact-root ./mlruns --host 0.0.0.0 --port 5000

    http://10.200.2.51:5000/#/experiments/0

    Sử dụng SQLite làm backend store. Dữ liệu của MLflow (chẳng hạn như thông tin về các experiments, runs, parameters, metrics) sẽ được lưu trong tệp mlflow.db.
    

    mlflow server --host 10.200.2.51 --port 5000

create a ML model with mlflow tracker:
    mlflow.set_tracking_uri('http://10.200.2.51:5000')
    mlflow.set_experiment('exp-1')