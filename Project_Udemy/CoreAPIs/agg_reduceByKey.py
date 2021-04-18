# reduceByKey()

from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext(master='local', appName="CoreAPIs")

    order_items = sc.textFile("/user/spark/retail_db/order_items/part-00000")

    # order Revenue
    order_pair = order_items.map(lambda oi: (int(oi.split(",")[1]), round(float(oi.split(",")[4]), 2)))
    order_revenue = order_pair.reduceByKey(lambda x, y: x + y)
    for i in order_revenue.take(10):
        print(i)

    # Get min revenue for each order id
    order_pair = order_items.map(lambda oi: (int(oi.split(",")[1]), round(float(oi.split(",")[4]), 2)))
    order_revenue = order_pair.reduceByKey(lambda x, y: min(x, y))
    for i in order_revenue.take(10):
        print(i)

    # Get order item details with minimum subtotal for each order
    order_pair = order_items.map(lambda oi: (int(oi.split(",")[1]), oi))
    order_min_total = order_pair.reduceByKey(lambda x,y:x if float(x.split(",")[4]) <= float(y.split(",")[4]) else y)
    for i,j in order_min_total.take(10):
        print(j)