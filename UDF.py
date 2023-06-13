from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, expr
from pyspark.sql.types import StringType

my_conf = SparkConf()
my_conf.set("spark.app.name","my first application")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

orderDF = spark.read\
    .format("csv")\
    .option("inferschema",True)\
    .option("path","C:/Users/atulg/Downloads/Spark_datasets_w12/dataset1.csv")\
    .load()


orderDF1 = orderDF.toDF("name","age","city")

def agecheck(age):
    if(age>18):
        return "Y"
    else:
        return "N"


spark.udf.register("parseAgecheck",agecheck,StringType())

DF2 = orderDF1.withColumn("adult",expr("parseAgecheck(age)"))

DF2.printSchema()
DF2.show()
