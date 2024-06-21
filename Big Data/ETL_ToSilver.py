from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import subprocess

def raw_to_silver():
    # Crear sesión de Spark
    spark = SparkSession \
        .builder \
        .appName("Silver Zone Creation") \
        .master("local[*]") \
        .getOrCreate()

    # Definir la ruta base en HDFS
    hdfs_base_path = 'hdfs://localhost:9000'

    # Leer cada archivo Parquet
    #df_50001 = spark.read.parquet(hdfs_base_path + '/data/50001.parquet')
    #df_customers = spark.read.parquet(hdfs_base_path + '/data/customers.parquet')
    df_data_raw = spark.read.parquet(hdfs_base_path + '/raw/new/*.parquet')
    #df_employees = spark.read.parquet(hdfs_base_path + '/data/employees.parquet')
    df_neighborhoods = spark.read.parquet(hdfs_base_path + '/data/medellin_neighborhoods.parquet')

    # Crear la base de datos bronze si no existe
    #spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    # Guardar los datos como una tabla en la base de datos bronze
    #df_data_raw.write.mode('append').saveAsTable('bronze.almacenamiento_bronze')

    df_silver = df_data_raw.withColumn("event_date", to_timestamp(col("date"), "dd/MM/yyyy HH:mm:ss")) \
        .withColumn("partition_date", date_format(col("event_date"), "ddMMyyyy")) \
        .withColumn("event_day", dayofmonth(col("event_date"))) \
        .withColumn("event_hour", hour(col("event_date"))) \
        .withColumn("event_minute", minute(col("event_date"))) \
        .withColumn("event_month", month(col("event_date"))) \
        .withColumn("event_second", second(col("event_date"))) \
        .withColumn("event_year", year(col("event_date"))) \
        .withColumnRenamed("order_id", "order_id") \
        .withColumnRenamed("employee_id", "employee_id") \
        .withColumnRenamed("quantity_products", "quantity_products") \
        .withColumnRenamed("latitude", "latitude") \
        .withColumnRenamed("longitude", "longitude") \
        .withColumnRenamed("customer_id", "customer_id")

    # Eliminar la columna original de fecha si no es necesaria
    df_silver = df_silver.drop("date")

    # Seleccionar solo las columnas NOMBRE e IDENTIFICACION de df_neighborhoods, y renombrar OBJECTID a object_id
    df_neighborhoods_seleccionado = df_neighborhoods.select(
        col("OBJECTID").alias("object_id"),
        col("NOMBRE").alias("neighborhood"),
        col("IDENTIFICACION").alias("commune")
    )

    # Realizar el left join en la columna object_id
    df_resultado = df_silver.join(
        df_neighborhoods_seleccionado,
        df_silver['object_id'] == df_neighborhoods_seleccionado['object_id'],
        "left"
    ).select(
        df_silver['*'],  # Seleccionar todas las columnas de df_silver
        df_neighborhoods_seleccionado['neighborhood'],  # Seleccionar la columna renombrada neighborhood
        df_neighborhoods_seleccionado['commune']  # Seleccionar la columna renombrada commune
    ).drop(df_silver['object_id'])  # Eliminar la columna object_id del resultado final

    # Guardar datos procesados en la zona Silver
    current_time = datetime.now().strftime("%Y%m%d%H%M%S")
    output_path = "/silver/new/data_silver_"+current_time+".parquet"
    df_resultado.write.mode('overwrite').parquet(output_path)

    # Mover a carpeta de procesados
    src_path = "/raw/new/*.parquet"
    dest_path = "/raw/processed/"
    cmd = f"hdfs dfs -mv {src_path} {dest_path}"
    result = subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # Detener sesión de Spark
    spark.stop()

raw_to_silver()