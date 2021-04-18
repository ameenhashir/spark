# groupByKey()

from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext(master='local', appName="CoreAPIs")

    order_items = sc.textFile("/user/spark/retail_db/order_items/part-00000")

    #revenue per order id
    order_item_pair = order_items.map(lambda oi:(int(oi.split(",")[1]),float(oi.split(",")[4])))
    order_revenue = order_item_pair.groupByKey()
    order_rev_rnd = order_revenue.map(lambda r:(r[0],round(sum(r[1]),2)))
    for ord,r in order_rev_rnd.take(10):
        print(f'{ord} - {r}')

    # Get order item details in descending order by revenue
    order_item_pair = order_items.map(lambda oi: (int(oi.split(",")[1]), oi))
    order_item_pair_grp = order_item_pair.groupByKey()
    print("using map")
    order_item_pair_grp_srt = order_item_pair_grp.map(lambda oi:sorted(oi[1],key=lambda v:float(v.split(",")[4]),reverse=True))
    for i in order_item_pair_grp_srt.take(10):
        print(i)
    print("using flatMap")
    order_item_pair_grp_srt = order_item_pair_grp.flatMap(lambda oi: sorted(oi[1], key=lambda v: float(v.split(",")[4]), reverse=True))
    for i in order_item_pair_grp_srt.take(10):
        print(i)
