# Union
# intersection
# distinct
# subtract

from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext(master='local', appName="CoreAPIs")

    # order items of 2014-01 and 2013-12

    order_items = sc.textFile("/user/spark/retail_db/order_items/part-00000")
    orders = sc.textFile("/user/spark/retail_db/orders/part-00000")

    order_items_key = order_items.map(lambda oi: (int(oi.split(",")[1]), oi))
    orders_key = orders.map(lambda o: (int(o.split(",")[0]), o))
    order_item_201312 = order_items_key.join(orders_key).filter(lambda oi: oi[1][1].split(",")[1][:7] == '2013-12').map(lambda oi:oi[1][0])
    order_item_201401 = order_items_key.join(orders_key).filter(lambda oi: oi[1][1].split(",")[1][:7] == '2014-01').map(lambda oi:oi[1][0])

    products_201312 = order_item_201312.map(lambda oi:oi.split(",")[2])
    products_201401 = order_item_201401.map(lambda oi:oi.split(",")[2])

    print(products_201312.count())
    print(products_201401.count())

    #all products
    all_poducts = products_201312.union(products_201401)
    all_poducts_distinct = products_201312.union(products_201401).distinct()

    print(all_poducts.count())
    print(all_poducts_distinct.count())

    #common products
    common = products_201312.intersection(products_201401)
    print(common.count())

    #diff
    diff = products_201312.subtract(products_201401)
    print(diff.count())