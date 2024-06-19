from pyspark.sql import SparkSession

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("Gold Zone Creation") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

# Leer datos de la zona Silver
silver_df = spark.read.parquet("hdfs://namenode:9000/silver/silver_zone_data.parquet")

# Leer datos complementarios
customers_df = spark.read.parquet("hdfs://namenode:9000/data/customers.parquet")
employees_df = spark.read.parquet("hdfs://namenode:9000/data/employees.parquet")

# Agregar información de clientes
silver_df = silver_df.join(customers_df, silver_df.customer_id == customers_df.id, "left") \
    .select(silver_df["*"], customers_df["name"].alias("customer_name"), customers_df["phone"].alias("customer_phone"), customers_df["email"].alias("customer_email"), customers_df["address"].alias("customer_address"))

# Agregar información de empleados
silver_df = silver_df.join(employees_df, silver_df.employee_id == employees_df.id, "left") \
    .select(silver_df["*"], employees_df["name"].alias("employee_name"), employees_df["phone"].alias("employee_phone"), employees_df["email"].alias("employee_email"), employees_df["address"].alias("employee_address"), employees_df["commission"].alias("employee_commission"))

# Ventas por comuna y barrio
commune_sales_df = silver_df.groupBy("commune", "neighborhood") \
    .agg({"quantity_products": "sum", "sale_value": "sum"}) \
    .withColumnRenamed("sum(quantity_products)", "total_products") \
    .withColumnRenamed("sum(sale_value)", "total_sales")

# Rendimiento de empleados
employee_sales_df = silver_df.groupBy("employee_id", "employee_name", "employee_phone", "employee_email", "employee_address", "employee_commission") \
    .agg({"quantity_products": "sum", "sale_value": "sum"}) \
    .withColumnRenamed("sum(quantity_products)", "total_products") \
    .withColumnRenamed("sum(sale_value)", "total_sales")

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

# Guardar datos procesados en la zona Gold
commune_sales_df.write.mode('overwrite').parquet("hdfs://namenode:9000/gold/commune_sales.parquet")
employee_sales_df.write.mode('overwrite').parquet("hdfs://namenode:9000/gold/employee_sales.parquet")
detailed_sales_df.write.mode('overwrite').parquet("hdfs://namenode:9000/gold/detailed_sales.parquet")
