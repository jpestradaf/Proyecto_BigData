from pyspark.sql import SparkSession
from datetime import datetime
import os

def silver_to_gold():
    # Crear sesión de Spark
    spark = SparkSession.builder \
        .appName("Gold Zone Creation") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()

    # Leer datos de la zona Silver
    silver_path = "hdfs://namenode:9000/silver/new"
    silver_df = spark.read.parquet(silver_path)

    # Leer datos complementarios
    customers_df = spark.read.parquet("hdfs://namenode:9000/data/customers.parquet")
    employees_df = spark.read.parquet("hdfs://namenode:9000/data/employees.parquet")

    # Agregar información de clientes
    silver_df = silver_df.join(customers_df, silver_df.customer_id == customers_df.id, "left") \
        .select(silver_df["*"], customers_df["name"].alias("customer_name"), customers_df["phone"].alias("customer_phone"), customers_df["email"].alias("customer_email"), customers_df["address"].alias("customer_address"))

    # Agregar información de empleados
    silver_df = silver_df.join(employees_df, silver_df.employee_id == employees_df.id, "left") \
        .select(silver_df["*"], employees_df["name"].alias("employee_name"), employees_df["phone"].alias("employee_phone"), employees_df["email"].alias("employee_email"), employees_df["address"].alias("employee_address"), employees_df["commission"].alias("employee_commission"))

    # Ventas detalladas
    detailed_sales_df = silver_df.select(
        "order_id", 
        "customer_id", 
        "customer_name", 
        "customer_phone", 
        "customer_email", 
        "customer_address", 
        "employee_id", 
        "employee_name", 
        "employee_phone", 
        "employee_email", 
        "employee_address", 
        "employee_commission", 
        "commune", 
        "neighborhood", 
        "quantity_products", 
        "sale_value", 
        "event_date"
    )

    # Obtener la fecha y hora actual para el nombre del archivo
    current_time = datetime.now().strftime("%Y%m%d%H%M%S")
    output_path = f"hdfs://namenode:9000/gold/detailed_sales_{current_time}.parquet"

    # Guardar datos procesados en la zona Gold
    detailed_sales_df.write.mode('overwrite').parquet(output_path)

    # Mover archivos de silver/new a silver/processed
    processed_path = "hdfs://namenode:9000/silver/processed"

    # Listar los archivos en la ruta silver/new
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    files_to_move = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(silver_path))

    # Mover cada archivo
    for file in files_to_move:
        old_path = file.getPath()
        new_path = spark._jvm.org.apache.hadoop.fs.Path(os.path.join(processed_path, old_path.getName()))
        fs.rename(old_path, new_path)

    # Detener sesión de Spark
    spark.stop()

# Llamar a la función
silver_to_gold()
