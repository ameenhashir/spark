from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.functions import *

if __name__ == "__main__":

    spark = SparkSession.\
        builder.\
        appName("Read from socket").\
        master("local[*]").\
        getOrCreate()

    data_read_df = spark.\
                readStream. \
                format("socket").\
                option("host","localhost").\
                option("port",1002).\
                load()

    print(data_read_df.isStreaming)
    data_read_df.printSchema()

    word_count_df = data_read_df.select(explode(split("value",' ')).alias("word")).groupBy("word").count()

    write_stream = word_count_df.\
                writeStream. \
                outputMode("complete"). \
                format("console").\
                trigger(processingTime="2 second").\
                start()

    write_stream.awaitTermination()
    print("Program completed!!!")

