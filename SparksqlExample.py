from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name","my first application")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).enableHiveSupport.getOrCreate()

orderDf = spark.read\
    .format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","C:/Users/atulg/Downloads/Spark_datasets_w12/orders-201025-223502.csv")\
    .load()


orderDf.createOrReplaceTempView("orders")

resultDF = spark.sql("select order_status , count(*) as total_orders  from orders group by order_status")

#spark.sql("create database if not exists retail")
#orderDf.write.format("csv").mode("overwrite").saveAsTable("retail.orders")

resultDF.show()

