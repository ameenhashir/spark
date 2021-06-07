from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.functions import *

if __name__ == "__main__":

    spark = SparkSession.\
        builder.\
        appName("Read from file csv").\
        master("local[*]").\
        getOrCreate()

    data_read_df = spark.\
                readStream. \
                format("csv").\
                option("header",'true').\
                schema("registration_dttm String,id Int,first_name String,last_name String,email String,gender String,ip_address String,cc String,country String,birthdate String,salary Float,title String,comments String").\
                load("../input/csv")

    print(data_read_df.isStreaming)
    data_read_df.printSchema()

    write_stream = data_read_df. \
        writeStream. \
        format("console"). \
        trigger(processingTime="2 second"). \
        start()

    write_stream.awaitTermination()
    print("Program completed!!!")