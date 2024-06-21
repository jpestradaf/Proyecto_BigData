from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from datetime import datetime
import subprocess
import os

def silver_to_gold():
    # Crear sesión de Spark
    spark = SparkSession.builder \
        .appName("Gold Zone Creation") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .getOrCreate()

    # Verificar la existencia de archivos en la ruta
    silver_path = "hdfs://localhost:9000/silver/new/"
    try:
        files = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()).globStatus(spark._jvm.org.apache.hadoop.fs.Path(silver_path + "*.parquet"))
        if files:
            silver_df = spark.read.parquet(silver_path + "*.parquet")

            # Crear las tablas en SQL en gold
            spark.sql("CREATE DATABASE IF NOT EXISTS silver")
            detailed_sales_df.write.mode('append').saveAsTable('silver.almacenamiento_silver')
            
            # Leer datos complementarios
            customers_df = spark.read.parquet("hdfs://localhost:9000/data/customers.parquet")
            employees_df = spark.read.parquet("hdfs://localhost:9000/data/employees.parquet")

            # Agregar información de clientes
            silver_df = silver_df.join(customers_df, silver_df.customer_id == customers_df.id, "left") \
                .select(silver_df["*"], customers_df["name"].alias("customer_name"), customers_df["phone"].alias("customer_phone"), customers_df["email"].alias("customer_email"), customers_df["address"].alias("customer_address"))

            # Agregar información de empleados
            silver_df = silver_df.join(employees_df, silver_df.employee_id == employees_df.id, "left") \
                .select(silver_df["*"], employees_df["name"].alias("employee_name"), employees_df["phone"].alias("employee_phone"), employees_df["email"].alias("employee_email"), employees_df["address"].alias("employee_address"), employees_df["commission"].alias("employee_commission"))

            # Ventas detalladas
            detailed_sales_df = silver_df.select(
                "order_id", 
                # "customer_id", 
                "customer_name", 
                # "customer_phone", 
                # "customer_email", 
                # "customer_address", 
                # "employee_id", 
                "employee_name", 
                # "employee_phone", 
                # "employee_email", 
                # "employee_address", 
                "employee_commission", 
                "latitude",
                "longitude",
                "commune", 
                "neighborhood", 
                "quantity_products", 
                "event_date"
            )

            # Obtener la fecha y hora actual para el nombre del archivo
            current_time = datetime.now().strftime("%Y%m%d%H%M%S")
            output_path = f"hdfs://localhost:9000/gold/detailed_sales_{current_time}.parquet"

            # Guardar datos procesados en la zona Gold
            detailed_sales_df.write.mode('overwrite').parquet(output_path)

            # Crear las tablas en SQL en gold
            spark.sql("CREATE DATABASE IF NOT EXISTS gold")
            detailed_sales_df.write.mode('append').saveAsTable('gold.almacenamiento_gold')

            # Mover cada archivo
            src_path = "hdfs://localhost:9000/silver/new/*.parquet"
            dest_path = "hdfs://localhost:9000/silver/processed/"
            cmd = f"hdfs dfs -mv {src_path} {dest_path}"
            result = subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            # Mover archivos de silver/new a silver/processed
            #processed_path = "hdfs://localhost:9000/silver/processed"

            # Listar los archivos en la ruta silver/new
            #fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
            #files_to_move = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(silver_path))

            # Mover cada archivo
            #for file in files_to_move:
                #old_path = file.getPath()
                #new_path = spark._jvm.org.apache.hadoop.fs.Path(os.path.join(processed_path, old_path.getName()))
                #fs.rename(old_path, new_path)


        else:
            print(f"No hay archivos .parquet en {silver_path}")
            # Puedes decidir cómo manejar esta situación, por ejemplo, registrar un error o realizar alguna acción alternativa.

    except AnalysisException as e:
        print(f"Error al leer archivos .parquet en {silver_path}: {str(e)}")    

    # Detener sesión de Spark
    spark.stop()

# Llamar a la función
silver_to_gold()
