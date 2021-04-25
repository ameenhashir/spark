from pyspark import SparkContext,SparkConf

sc = SparkContext(master="spark://192.168.56.104:7077",appName="Spark Demo")
print(sc.textFile("/user/spark/data/retail_db/products/part-00000").first())