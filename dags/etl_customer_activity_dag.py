from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import os

# Параметры
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def extract(**kwargs):
    """Извлечение данных из profit_table.csv."""
    filepath = '/opt/airflow/dags/profit_table.csv'  # Укажите путь к файлу
    df = pd.read_csv(filepath)
    kwargs['ti'].xcom_push(key='data', value=df.to_dict())

def transform(**kwargs):
    """Преобразование данных."""
    from transform_script import transform  # Импортируем функцию transform
    ti = kwargs['ti']
    data = ti.xcom_pull(key='data', task_ids='extract')
    df = pd.DataFrame.from_dict(data)
    result_df = transform(df)  # Вызываем функцию transform
    kwargs['ti'].xcom_push(key='transformed_data', value=result_df.to_dict())

def load(**kwargs):
    """Загрузка данных в flags_activity.csv."""
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(key='transformed_data', task_ids='transform')
    df = pd.DataFrame.from_dict(transformed_data)
    output_path = '/opt/airflow/dags/flags_activity.csv'  # Укажите путь для сохранения
    if os.path.exists(output_path):
        existing_df = pd.read_csv(output_path)
        df = pd.concat([existing_df, df]).drop_duplicates()
    df.to_csv(output_path, index=False)

# Определяем DAG
with DAG(
    'etl_customer_activity',
    default_args=default_args,
    description='ETL процесс для расчёта витрины активности клиентов',
    schedule_interval='0 0 5 * *',  # Запускать 5-го числа каждого месяца
    start_date=datetime(2023, 12, 1),
    catchup=False,
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
        provide_context=True,
    )
    
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
        provide_context=True,
    )
    
    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
        provide_context=True,
    )

    extract_task >> transform_task >> load_task
