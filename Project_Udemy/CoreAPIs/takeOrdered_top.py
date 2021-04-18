# aggregateByKey()

from pyspark import SparkContext
from re import compile


class Utils:
    comma_delimiter = compile(',(?=(?:[^"]*"[^"]*")*[^"]*$)')


if __name__ == "__main__":
    sc = SparkContext(master='local', appName="CoreAPIs")

    products = sc.textFile("/user/spark/retail_db/products/part-00000")
    print('Method1')
    product_ordered = products.takeOrdered(10, lambda x: float(Utils.comma_delimiter.split(x)[4]))
    for i in product_ordered:
        print(i)

    print('Method2')
    product_ordered = products.takeOrdered(10, lambda x: -1 * float(Utils.comma_delimiter.split(x)[4]))
    for i in product_ordered:
        print(i)

    print('Method3')
    product_ordered = products.top(10, lambda x: float(Utils.comma_delimiter.split(x)[4]))
    for i in product_ordered:
        print(i)