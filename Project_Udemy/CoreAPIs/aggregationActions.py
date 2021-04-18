# count()
# reduce()
# countByKey()

from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext(master='local', appName="CoreAPIs")

    order_items = sc.textFile("/user/spark/retail_db/order_items/part-00000")
    orders = sc.textFile("/user/spark/retail_db/orders/part-00000")

    # Total Orders
    total_orders = orders.count()
    print("Total Orders : ", total_orders)

    # Total Revenue
    total_revenue = order_items.map(lambda oi: float(oi.split(",")[4])).reduce(lambda x, y: x + y)
    print("Total Revenue : ", total_revenue)

    # max revenue order item from an order
    order_id = int(input("Enter order_id : "))
    order_item = order_items.filter(lambda oi: int(oi.split(",")[1]) == order_id)
    for o in order_item.collect():
        print(o)
    order_item_max = order_item.reduce(lambda x, y: x if float(x.split(",")[4]) >= float(y.split(",")[4]) else y)
    print("Max revenue",order_item_max)

    # Orders status count
    order_status = orders.map(lambda o:(o.split(",")[3],1)).countByKey()
    print("Order Status:")
    for s,i in order_status.items():
        print(s + '-' + str(i))