import sys
sys.path.insert(0, '/workspace/')  # Añadir la ruta al inicio del PYTHONPATH

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Importar la función de ETL
from ETL_ToGold import silver_to_gold

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
    'silver_to_gold_dag',
    default_args=default_args,
    description='ETL workflow to process data from Silver to Gold',
    schedule_interval=timedelta(hours=1),  # Ejecutar cada hora
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Definir la tarea para procesar de Silver a Gold
    task_silver_to_gold = PythonOperator(
        task_id='silver_to_gold',
        python_callable=silver_to_gold,
    )
