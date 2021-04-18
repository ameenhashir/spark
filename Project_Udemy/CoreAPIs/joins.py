from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext(master='local',appName="CoreAPIs")

    order_items = sc.textFile("/user/spark/retail_db/order_items/part-00000")
    orders = sc.textFile("/user/spark/retail_db/orders/part-00000")

    order_revenue = order_items.map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))
    order_status = orders.map(lambda o: (int(o.split(",")[0]), o.split(",")[3]))

    order_rev_status = order_revenue.join(order_status)

    for rec in order_rev_status.take(10):print(rec)