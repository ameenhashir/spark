# aggregateByKey()

from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext(master='local', appName="CoreAPIs")

    order_items = sc.textFile("/user/spark/retail_db/order_items/part-00000")
    order_items_pair = order_items.map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))
    order_rev = order_items_pair.aggregateByKey((0.0, 0),
                                                lambda x, y: (x[0] + y, x[1] + 1),
                                                lambda x, y: (x[0] + y[0], x[1] + y[1]))
    for i in order_rev.take(10):
        print(i)

    order_items_pair = order_items.map(lambda oi: (int(oi.split(",")[1]), (float(oi.split(",")[4]), 1)))
    order_rev = order_items_pair.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    for i in order_rev.take(10):
        print(i)
