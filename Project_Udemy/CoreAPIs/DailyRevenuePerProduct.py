# Use retail_db data set
# Problem Statement
# Get Daily revenue by product considering completed and closed orders
# Data need to be sorted by ascending order by date and then descending order by revenue computed for each product for ech day
# Data should be delimited by "," in this order [order_date,daily_revenue_per_product,product_name]
# Data for orders and order_items is available in HDFS
# /orders
# /order_items
# Data for products is available locaclly under
# /products
# Final output need to ne stored under
# HDFS location -avro format
# /daily_revenue_avro_python
# HDFS location - text for,at
# /daily_revenue_python
# Local location
# /daily_revenue_python
# Solution need to be stored under
# /daily_revenue_python.txt

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from operator import add
import re

class Utils:
    comma_delimiter = re.compile(',(?=(?:[^"]*"[^"]*")*[^"]*$)')


conf = SparkConf().setAppName("DailyRevenueByProduct").setMaster('local')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

order_items = sc.textFile("/user/spark/retail_db/order_items/part-00000")
orders = sc.textFile("/user/spark/retail_db/orders/part-00000")

order_items_pair = order_items.map(lambda oi: (int(oi.split(",")[1]), (int(oi.split(",")[2]), float(oi.split(",")[4]))))
orders_pair = orders.filter(lambda o: o.split(",")[3] in ('COMPLETE', 'CLOSED')).map(lambda o: (int(o.split(',')[0]),o.split(",")[1].split(" ")[0]))
order_revenue = order_items_pair.join(orders_pair)
order_rev_daily = order_revenue.map(lambda o:((o[1][0][0],o[1][1]),o[1][0][1])).reduceByKey(add).map(lambda o:(int(o[0][0]),(o[0][1],round(o[1],2))))

produts_raw = open("E:\\Technologies\\ApacheSpark\\data\\retail_db\\products\\part-00000").read().splitlines()
products = sc.parallelize(produts_raw).map(lambda p:(int(Utils.comma_delimiter.split(p)[0]),Utils.comma_delimiter.split(p)[2]))
product_rev_daily_prod = order_rev_daily.leftOuterJoin(products).map(lambda p:((p[1][0][0],-1 * p[1][0][1]),p[1][1])).sortByKey(ascending=True)
product_revenue = product_rev_daily_prod.map(lambda p:(p[0][0],-p[0][1],p[1]))

pr_DF = product_revenue.toDF(schema=['ORDER_DATE','REVENUE','PRODUCT_NAME'])
print(pr_DF.show())

pr_DF.write.json("/user/spark/output/daily_revenue_json1")