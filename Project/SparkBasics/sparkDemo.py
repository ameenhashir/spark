from pyspark import SparkContext,SparkConf

sc = SparkContext(master="local",appName="Spark Demo")
print(sc.textFile("E:\Technologies\ApacheSpark\data\cards\deckofcards.txt").first())