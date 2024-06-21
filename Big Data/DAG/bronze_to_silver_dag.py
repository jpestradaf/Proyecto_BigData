import sys
sys.path.insert(0, '/workspace/')  # Añadir la ruta al inicio del PYTHONPATH

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Importar la función de ETL
from ETL_ToSilver import bronze_to_silver

# Definir los argumentos del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Crear el DAG
with DAG(
    'bronze_to_silver_dag',
    default_args=default_args,
    description='ETL workflow to process data from Bronze to Silver',
    schedule_interval=timedelta(minutes=30),  # Ejecutar cada 30 minutos
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Definir la tarea para procesar de Bronze a Silver
    task_bronze_to_silver = PythonOperator(
        task_id='bronze_to_silver',
        python_callable=bronze_to_silver,
    )
