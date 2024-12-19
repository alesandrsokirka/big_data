from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

def extract():
    # Логика для извлечения данных
    print("Extracting data...")
    return True

def transform():
    # Логика для преобразования данных
    print("Transforming data...")
    # Например, если возникает ошибка, выбрасываем исключение
    # raise ValueError("Test exception")
    return True

def load():
    # Логика для загрузки данных
    print("Loading data...")
    return True
# Определяем параметры DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your_email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Создаем DAG
with DAG(
    dag_id='etl_customer_activity',
    default_args=default_args,
    description='ETL DAG for customer activity',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 12, 1),
    catchup=False,
    tags=['example'],
) as dag:

    start = DummyOperator(
        task_id='start',
    )

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
        retries=3,  # Увеличиваем количество повторных попыток
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    end = DummyOperator(
        task_id='end',
    )

    # Задаем порядок выполнения задач
    start >> extract_task >> transform_task >> load_task >> end
