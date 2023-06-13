from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name","my first application")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

orderDf = spark.read\
    .format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","C:/Users/atulg/Downloads/Spark_datasets_w12/orders-201025-223502.csv")\
    .load()
#print("number of partions are ",orderDf.rdd.getNumPartitions())

#ordersRep = orderDf.repartition(4)


orderDf.write\
    .format("csv")\
    .partitionBy("order_status")\
    .mode("overwrite")\
    .option("path","C:/Users/atulg/Downloads/Spark_datasets_w12/pyspark1")\
    .save()




