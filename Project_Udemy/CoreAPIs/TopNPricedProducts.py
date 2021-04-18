from pyspark import SparkContext
from re import compile
from itertools import takewhile


class Utils:
    comma_delimiter = compile(',(?=(?:[^"]*"[^"]*")*[^"]*$)')


def getTopN(p, n):
    products_sorted = sorted(p, key=lambda p: float(Utils.comma_delimiter.split(p)[4]), reverse=True)
    topNPrices = sorted(set(map(lambda x: float(Utils.comma_delimiter.split(x)[4]), products_sorted)), reverse=True)[:n]
    return takewhile(lambda x: float(Utils.comma_delimiter.split(x)[4]) in topNPrices, products_sorted)


if __name__ == "__main__":
    sc = SparkContext(master='local', appName="CoreAPIs")
    products = sc.textFile("/user/spark/retail_db/products/part-00000")
    products_pair = products.map(lambda p: (int(Utils.comma_delimiter.split(p)[1]), p))
    products_topn = products_pair.groupByKey().flatMap(lambda o: getTopN(o[1], 3))
    for i in products_topn.take(10):
        print(i)