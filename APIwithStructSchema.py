from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StringType, TimestampType, StructType

my_conf = SparkConf()
my_conf.set("spark.app.name","my first application")
my_conf.set("spark.master","local[*]")

orderSchema= StructType([
    StructField("order_id",IntegerType()),
    StructField("orderdate",TimestampType()),
    StructField("customerid",IntegerType()),
    StructField("status",StringType())
])

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

orderDf = spark.read\
    .format("csv")\
    .option("header",True)\
    .schema(orderSchema,True)\
    .option("path","C:/Users/atulg/Downloads/Spark_datasets_w11/orders-201019-002101.csv")\
    .load()


orderDf.printSchema()



spark.stop()



