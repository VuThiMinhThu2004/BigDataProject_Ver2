from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2025, 3, 24, 10, 00),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
    
}

def get_data():
    import requests
    
    res = requests.get('https://randomuser.me/api/').json()
    res = res['results'][0]
    
    return res

def format_data(res):
    data = {}
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['postcode'] = res['location']['postcode']
    data['email'] = res['email']
    data['phone'] = res['phone']
    
    return data
    
def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging
    
    producer = KafkaProducer(bootstrap_servers=['broker:29092'],
                            max_block_ms=5000)
    curr_time = time.time()

    
    while True:
        if time.time() > curr_time + 60: # More than 1 minute
            break
        try:
            res = get_data()
            
            res = format_data(res)
            
            producer.send('users.created', json.dumps(res).encode('utf-8'))
            
            time.sleep(5)
            
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue
    
    producer.flush()
    producer.close()
    
with DAG('user_automation', 
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    # Streaming data to Kafka
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
    
    # # Trigger Spark Job to process Kafka messaages then send to PostgreSQL
    # trigger_spark_job = BashOperator(
    #     task_id='trigger_spark_job',
    #     bash_command=(
    #         "docker exec -it spark-master spark-submit "
    #         "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.7.4 "
    #         "/opt/spark_app/spark_stream.py"
    #     )
    # )
    
    streaming_task




