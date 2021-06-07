from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.functions import *

if __name__ == "__main__":

    spark = SparkSession.\
        builder.\
        appName("Read from file json").\
        master("local[*]").\
        getOrCreate()

    data_read_df = spark.\
                readStream. \
                format("json").\
                option("header",'true').\
                schema("registration_dttm String,id Int,first_name String,last_name String,email String,gender String,ip_address String,cc String,country String,birthdate String,salary Float,title String,comments String").\
                load("../input/json")

    spark.sparkContext.setLogLevel("ERROR")

    print(data_read_df.isStreaming)
    data_read_df.printSchema()

    word_count_df = data_read_df.groupBy("country").count().orderBy("count",ascending=False).limit(10)

    write_stream = word_count_df. \
        writeStream. \
        format("console"). \
        outputMode("complete").\
        option("checkpointLocation","checkpoint_json").\
        trigger(processingTime="2 second"). \
        start()

    write_stream.awaitTermination()
    print("Program completed!!!")