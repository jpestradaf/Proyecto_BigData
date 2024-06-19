from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, unix_timestamp, from_unixtime

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("Data Enrichment") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

# Leer datos de la zona Bronze
bronze_df = spark.read.json("hdfs://namenode:9000/bronze/bronze_zone_data.json")
#--------------------------------------------------------OJO, SI ES PARQUET, CAMBIARLO


# Leer datos complementarios
customers_df = spark.read.parquet("hdfs://namenode:9000/data/customers.parquet")
employees_df = spark.read.parquet("hdfs://namenode:9000/data/employees.parquet")
neighborhoods_df = spark.read.parquet("hdfs://namenode:9000/data/neighborhoods.parquet")


# Función para encontrar la comuna y barrio a partir de las coordenadas    
'''
    #1. GENERAR PUNTO
    #2. MAPEO QUE ENCUENTRA LA FILA CORRESPONDIENTE PARA EL BARRIO Y COMUNA(JOIN)
    #3. PEGAR LOS DATOS ENCONTRADOS 
'''
def find_neighborhood(latitude, longitude): #ESTO NO SIRVE AUN, METER GEOSPARK
    return neighborhoods_df \
        .filter((col("latitude") == latitude) & (col("longitude") == longitude)) \
        .select("commune", "neighborhood") \
        .collect()


# Enriquecer datos
silver_df = bronze_df \
    .withColumn("event_date", from_unixtime(unix_timestamp(col("date"), "MM/dd/yyyy HH:mm:ss"))) \
    .withColumn("partition_date", from_unixtime(unix_timestamp(col("date"), "MM/dd/yyyy"), "yyyyMMdd")) \
    .withColumn("event_year", col("event_date").cast("string").substr(0, 4).cast("int")) \
    .withColumn("event_month", col("event_date").cast("string").substr(6, 2).cast("int")) \
    .withColumn("event_day", col("event_date").cast("string").substr(9, 2).cast("int")) \
    .withColumn("event_hour", col("event_date").cast("string").substr(12, 2).cast("int")) \
    .withColumn("event_minute", col("event_date").cast("string").substr(15, 2).cast("int")) \
    .withColumn("event_second", col("event_date").cast("string").substr(18, 2).cast("int"))

# Agregar comuna y barrio
for row in silver_df.collect():
    neighborhood_info = find_neighborhood(row["latitude"], row["longitude"])
    silver_df = silver_df.withColumn("commune", lit(neighborhood_info[0]["commune"])) \
                         .withColumn("neighborhood", lit(neighborhood_info[0]["neighborhood"]))

# Guardar datos enriquecidos en la zona Silver
silver_df.write.mode('overwrite').parquet("hdfs://namenode:9000/silver/silver_zone_data.parquet")
