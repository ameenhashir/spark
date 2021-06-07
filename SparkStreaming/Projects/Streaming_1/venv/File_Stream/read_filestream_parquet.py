from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.functions import *

if __name__ == "__main__":

    spark = SparkSession.\
        builder.\
        appName("Read from file parquet").\
        config("spark.sql.streaming.schemaInference",'true').\
        master("local[*]").\
        getOrCreate()

    data_read_df = spark.\
                readStream. \
                format("parquet").\
                option("header",'true').\
                load("../input/parquet")

    print(data_read_df.isStreaming)
    data_read_df.printSchema()

    write_stream = data_read_df. \
        writeStream. \
        format("console"). \
        outputMode("update").\
        option("checkpointLocation","checkpoint_parquet").\
        trigger(processingTime="2 second"). \
        start()

    write_stream.awaitTermination()
    print("Program completed!!!")