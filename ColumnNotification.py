from audioop import avg
from itertools import count
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct

my_conf = SparkConf()
my_conf.set("spark.app.name","my first application")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

orderDF = spark.read\
    .format("csv")\
    .option("header",True)\
    .option("inferschema",True)\
    .option("path","C:/Users/atulg/Downloads/Spark_datasets_w12/order_data-201025-223502.csv")\
    .load()

#column object notification

#orderDF.select(
 #   count("*").alias("RowCount"),
 #   sum("Quantity").alias("TotalQuantity"),
 #   avg("UnitPrice").alias("AvgPrice"),
 #   countDistinct("InvoiceNo").alias("countDistinct")).show()

#column string expression

orderDF.selectExpr(
"count(*) as rowcount",
      "sum(Quantity) as TotalQuantity",
      "avg(UnitPrice) as AvgPrice",
      "count(Distinct(InvoiceNo)) as CountDistinct"
).show()


orderDF.createOrReplaceTempView("sales")

spark.sql("select count(*),sum(Quantity),avg(UnitPrice),count(distinct(InvoiceNo)) from sales").show()

