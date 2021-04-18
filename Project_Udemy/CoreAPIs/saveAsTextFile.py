from pyspark import SparkContext
from operator import add

if __name__ == "__main__":
    sc = SparkContext(master='local', appName="CoreAPIs")

    order_items = sc.textFile("/user/spark/retail_db/order_items/part-00000")
    order_items_key = order_items.map(lambda oi:(int(oi.split(",")[1]),float(oi.split(",")[4])))
    order_revenue = order_items_key.reduceByKey(add)
    for i in order_revenue.take(10):
        print(i)

    #save file
    order_revenue.saveAsTextFile("/user/spark/output/order_revenue1")

    order_items = sc.textFile("/user/spark/output/order_revenue1/part-00000")
    for oi in order_items.take(10):
        print(oi)

    