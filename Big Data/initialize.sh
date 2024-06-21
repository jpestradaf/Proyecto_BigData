#!/bin/bash
#Paquetes requeridos
pip install geopandas
pip install apache-airflow

# Crear directorios en HDFS
hdfs dfs -mkdir -p /raw/{new,processed}
hdfs dfs -mkdir -p /silver/{new,processed}
hdfs dfs -mkdir /gold
hdfs dfs -mkdir /data

# Copiar archivos .parquet del local a HDFS
hdfs dfs -copyFromLocal /workspace/AuxData/50001.parquet /data
hdfs dfs -copyFromLocal /workspace/AuxData/customers.parquet /data
hdfs dfs -copyFromLocal /workspace/AuxData/employees.parquet /data
hdfs dfs -copyFromLocal /workspace/AuxData/medellin_neighborhoods.parquet /data

# Crear carpeta /root/airflow/dags si no existe
if [ ! -d "/root/airflow/dags" ]; then
    mkdir -p /root/airflow/dags
fi

# Copiar archivos dag.py en local a la carpeta de dags en el contenedor
cp /workspace/DAG/bronze_to_silver_dag.py /root/airflow/dags
cp /workspace/DAG/silver_to_gold_dag.py /root/airflow/dags
