from airflow import DAG
from ip_fetcher import ip_fetcher
from datetime import datetime, timedelta
from dim_fact_gen import dim_fact_generator
from dataframe_gen import dataframe_generator
from dataframe_transform import dataframe_transformer
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG('packets_data_processing',
         start_date=datetime(2024, 3, 15),
         schedule_interval='*/50 * * * *',
         default_args=default_args,
         catchup=False) as dag:
    
    task1 = PythonOperator(
        task_id='dataframe_gen',
        provide_context=True,
        python_callable=dataframe_generator,
    )
    
    task2 = PythonOperator(
        task_id='dataframe_transform',
        python_callable=dataframe_transformer,
        provide_context=True,
        op_kwargs={'data': '{{ task_instance.xcom_pull(task_ids="dataframe_gen") }}'}
    )
    
    task3 = PythonOperator(
        task_id='Ip_fetcher',
        python_callable=ip_fetcher,
        provide_context=True,
        op_kwargs={'data': '{{ task_instance.xcom_pull(task_ids="dataframe_gen") }}'}
    )
    
    task4 = PythonOperator(
        task_id='Dim_fact_generator',
        python_callable=dim_fact_generator,
        provide_context=True,
        op_kwargs={
                'data': '{{ task_instance.xcom_pull(task_ids="dataframe_transform") }}', 
                'ip_data': '{{ task_instance.xcom_pull(task_ids="Ip_fetcher") }}'
            }
    )
    
    task1 >> [task2, task3] >> task4