from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, monotonically_increasing_id


my_conf = SparkConf()
my_conf.set("spark.app.name","my first application")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

mylist = [(1,"2013-07-25",11599,"CLOSED"),
(2,"2013-07-25",256,"PENDING_PAYMENT"),
(3,"2013-07-25",12111,"COMPLETE"),
(4,"2013-07-25",8827,"CLOSED"),
(5,"2013-07-25",11318,"COMPLETE"),
(6,"2013-07-25",7130,"COMPLETE"),
(7,"2013-07-25",4530,"COMPLETE")]


ordersDF = spark.createDataFrame(mylist).toDF("order_id","order_date","order_cust_id","status")
ordersDF.show()

newDF = ordersDF.withColumn("date1",unix_timestamp(col("order_date")))\
          .withColumn("newID",monotonically_increasing_id())\
          .drop_duplicates(["order_date","order_cust_id"])\
          .drop("order_id")\
          .sort("order_date")

newDF.printschema()
newDF.show()
