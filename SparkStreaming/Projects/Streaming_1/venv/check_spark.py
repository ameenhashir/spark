from pyspark.sql import SparkSession
from pyspark.sql.types import Row

if __name__ == "__main__":

    spark = SparkSession.\
        builder.\
        appName("pyspark check").\
        master("local[*]").\
        getOrCreate()

    data_row = [Row(id = 1,name='name1'),Row(id=2,name='name2')]

    data_df = spark.createDataFrame(data_row)

    data_df.show()

