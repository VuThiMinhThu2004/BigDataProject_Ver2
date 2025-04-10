import psycopg2

try:
    conn = psycopg2.connect(
        host="172.18.0.2",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    print("Connection successful!")
    conn.close()
except Exception as e:
    print(f"Error: {e}")