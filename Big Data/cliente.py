from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import json
from datetime import datetime

spark = SparkSession \
  .builder \
  .appName("streaming") \
  .master("local[*]") \
  .getOrCreate()

schema = ArrayType(StructType([
    StructField("object_id", IntegerType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("date", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("employee_id", IntegerType(), True),
    StructField("quantity_products", IntegerType(), True),
    StructField("order_id", StringType(), True)
]))

static_schema = StructType([
    StructField("object_id", IntegerType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("date", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("employee_id", IntegerType(), True),
    StructField("quantity_products", IntegerType(), True),
    StructField("order_id", StringType(), True)
])

static_df = spark.createDataFrame([], static_schema)
while True:
    streaming_df = spark.readStream.format("socket").option("host", "localhost").option("port", "8000").load()
    json_df = streaming_df.select(from_json(col("value"), schema).alias("data"))
    json_df = json_df.selectExpr("explode(data) as dict").select("dict.*")
    writing_df = json_df.writeStream.format("memory").queryName("socketData").outputMode("update").start()
    static_stream_df = spark.sql("SELECT * FROM socketData")
    static_df = static_df.union(static_stream_df)
    writing_df.awaitTermination(2)
    writing_df.stop()
    if static_df.count() > 50:
        now = datetime.now()
        now = now.strftime("%d%m%Y%H%M%S")
        ruta = '/raw/new/data_raw_'+now+'.parquet'
        print(ruta)
        static_df.write.mode('overwrite').parquet(ruta)
        static_df = spark.createDataFrame([], static_schema)
    time.sleep(30)