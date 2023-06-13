from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


my_conf = SparkConf()
my_conf.set("spark.app.name","my first application")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

orderDF = spark.read\
    .format("csv")\
    .option("header",True)\
    .option("inferschema",True)\
    .option("path","C:/Users/atulg/Downloads/Spark_datasets_w12/orders-201025-223502.csv")\
    .load()

orderDF.select("order_id","order_date").show()


orderDF.select(col("order_id")).show()

