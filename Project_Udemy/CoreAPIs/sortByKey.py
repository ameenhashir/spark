# sortByKey()

from pyspark import SparkContext
from re import compile


class Utils:
    comma_delimiter = compile(',(?=(?:[^"]*"[^"]*")*[^"]*$)')


if __name__ == "__main__":
    sc = SparkContext(master='local', appName="CoreAPIs")
    products = sc.textFile("/user/spark/retail_db/products/part-00000")
    for p in products.take(10):
        print(Utils.comma_delimiter.split(p))

    # sort data by product price descending
    product_pair = products.map(lambda x: (float(Utils.comma_delimiter.split(x)[4]), x))
    product_sort = product_pair.sortByKey(ascending=False).map(lambda x:x[1])
    for i in product_sort.take(10):
        print(i)
