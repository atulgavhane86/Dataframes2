from pyspark import SparkConf
from pyspark.sql import SparkSession


my_conf = SparkConf()
my_conf.set("spark.app.name","my first application")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

orderDf = spark.read.format("csv").option("header",True).option("inferSchema",True).option("path","C:/Users/atulg/Downloads/Spark_datasets_w11/orders-201019-002101.csv").load()

groupedDf = orderDf.repartition(4)\
    .where("Order_customer_id>10000")\
    .select("order_id","Order_customer_id")\
    .groupBy("Order_customer_id")\
    .count()

groupedDf.show()

#orderDf.printSchema()
#orderDf.show()

spark.stop()



