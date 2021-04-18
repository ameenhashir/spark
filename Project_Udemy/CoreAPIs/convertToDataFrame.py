from pyspark.sql import SparkSession
from pyspark import SparkContext
from operator import add

if __name__ == "__main__":
    sc = SparkContext(master='local', appName="CoreAPIs")
    spark = SparkSession(sc)

    order_items = sc.textFile("/user/spark/retail_db/order_items/part-00000")
    order_items_key = order_items.map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))
    order_revenue = order_items_key.reduceByKey(add).map(lambda r:(r[0],round(r[1],2)))
    for i in order_revenue.take(10):
        print(i)

    order_revenue_df=order_revenue.toDF(schema=['order_id','revenue'])
    print(order_revenue_df.show())

    for i in order_revenue_df.rdd.take(10):
        print(i)