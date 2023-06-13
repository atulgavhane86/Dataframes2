from pyspark import SparkContext , storagelevel

sc =  SparkContext("local[*]","joindemo")

rdd1 = sc.textFile("C:/Users/atulg/Downloads/Spark_datasets_w11/ratings-201019-002101.dat")

mapped_rdd = rdd1.map(lambda x:(x.split("::")[1],x.split("::")[2]))

new_mapped_rdd = mapped_rdd.mapValues(lambda x:(float(x),1.0))

reduced_rdd = new_mapped_rdd.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))

filtered_rdd = reduced_rdd.filter(lambda x: x[1][0] > 1000)


final_rdd = filtered_rdd.mapValues(lambda x : x[0]/x[1]).filter(lambda x:x[1]>4.5)


movies_rdd = sc.textFile("C:/Users/atulg/Downloads/Spark_datasets_w11/movies-201019-002101.dat")

movies_mapped_rdd = movies_rdd.map(lambda x: (x.split("::")[0],(x.split("::")[1],x.split("::")[2])))
joined_rdd = movies_mapped_rdd.join(final_rdd)

top_movies_rdd = joined_rdd.map(lambda x:x[1][0])

result = top_movies_rdd.collect()
for a in result:
    print(a)