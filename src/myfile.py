# -*- coding: utf-8 -*-
#!/usr/bin/env python
# Imports
import sys
import timeit
from datetime import datetime, timedelta
from decimal import *

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import abs, floor, udf
from pyspark.sql.types import (DecimalType, IntegerType, StringType,
                               StructField, StructType, TimestampType)

from settings import *

# Constantes
APP_NAME = "Data processing"


def day_close(date):
    """
    Returns the day of the week in int format
    Returns an int with the day of the week
    """
    return date.weekday()


CALCULATE_DAY = udf(day_close, IntegerType())


def main(spark, file, date, hour, time):
    """
    Method that processes the data
    :param spark: Spark instance
    :param file: Data File
    :param name: Name of the resulting file
    :param tiempos: Time file name
    """
    beginning = timeit.default_timer()
    # Column scheme of the csv that is received
    column_names = StructType([StructField("medallion", StringType(), True),
                               StructField("hack_license", StringType(), True),
                               StructField("vendor_id", StringType(), True),
                               StructField("rate_code", StringType(), True),
                               StructField("store_and_fwd_flag",
                                           StringType(), True),
                               StructField("pickup_datetime",
                                           TimestampType(), True),
                               StructField("dropoff_datetime",
                                           TimestampType(), True),
                               StructField("trip_time_in_secs",
                                           IntegerType(), True),
                               StructField("trip_distance", DecimalType(
                                   precision=10, scale=2), True),
                               StructField("pickup_longitude", DecimalType(
                                   precision=18, scale=14), True),
                               StructField("pickup_latitude", DecimalType(
                                   precision=18, scale=14), True),
                               StructField("dropoff_longitude", DecimalType(
                                   precision=18, scale=14), True),
                               StructField("dropoff_latitude", DecimalType(
                                   precision=18, scale=14), True)])
    data = spark.read.csv("./../data/unprocessed/" + file, schema=column_names,
                          timestampFormat="yyyy-MM-dd HH:mm:ss")
    data.createOrReplaceTempView("unprocessed")
    data.show()
    data.printSchema()

    # We filter the latitudes to eliminate invalid records
    # data = data.where(data.pickup_longitude >= INITIAL_LONGITUDE) \
    #     .where(data.pickup_longitude <= FINAL_LONGITUDE) \
    #     .where(data.dropoff_longitude >= INITIAL_LONGITUDE) \
    #     .where(data.dropoff_longitude <= FINAL_LONGITUDE) \
    #     .where(data.pickup_latitude >= FINAL_LATITUDE) \
    #     .where(data.pickup_latitude <= INITIAL_LATITUDE) \
    #     .where(data.dropoff_latitude >= FINAL_LATITUDE) \
    #     .where(data.dropoff_latitude <= INITIAL_LATITUDE)

    # We establish the grid system and calculate the day of the week
    data = data.withColumn("cuad_pickup_latitude",
                           floor((INITIAL_LATITUDE - data.pickup_latitude)/LATITUDE) + 1) \
        .withColumn("cuad_pickup_longitude",
                    floor(abs(INITIAL_LONGITUDE - data.pickup_longitude)/LONGITUDE) + 1) \
        .withColumn("cuad_dropoff_latitude",
                    floor((INITIAL_LATITUDE - data.dropoff_latitude)/LATITUDE) + 1) \
        .withColumn("cuad_dropoff_longitude",
                    floor(abs(INITIAL_LONGITUDE - data.dropoff_longitude)/LONGITUDE) + 1) \
        .withColumn("day_of_week", CALCULATE_DAY(data.pickup_datetime))

    end_time = get_timestamp(date, hour)
    start_time = end_time - timedelta(minutes=30)
    frequent = data.filter(data.pickup_datetime <= end_time) \
        .filter(data.pickup_datetime >= start_time) \
        .filter(data.dropoff_datetime <= end_time) \
        .filter(data.dropoff_datetime >= start_time) \
        .groupBy("cuad_pickup_longitude", "cuad_pickup_latitude",
                 "cuad_dropoff_longitude", "cuad_dropoff_latitude") \
        .count().orderBy("count", ascending=False)
    frequent.show()
    frequent = frequent.take(10)

    fin = timeit.default_timer()
    file = open("./../data/results/" + "frequentResults.txt", "a")
    file.write(str(start_time) + ", " + str(end_time) + ", ")
    for i in range(len(frequent)):
        file.write(str(i) + ": ")
        file.write("(" + str(frequent[i][0]) +
                   ", " + str(frequent[i][1]) + ") ")
        file.write("(" + str(frequent[i][2]) +
                   ", " + str(frequent[i][3]) + "), ")
    file.write(str(fin - beginning) + "\n")
    file.close()

    # fin = timeit.default_timer()
    # file = open("./../data/results/" + time + ".txt", "a")
    # file.write(str(fin - beginning) + "\n")
    # file.close()


if __name__ == "__main__":
    # Let's Configure SparkConf
    CONF = SparkConf()
    CONF.setAppName(APP_NAME)
    CONF.setMaster("local[*]")
    SPARK = SparkSession.builder.config(conf=CONF).getOrCreate()
    FILE = sys.argv[1]
    DATE = sys.argv[2]
    HOUR = sys.argv[3]
    TIME = sys.argv[4]
    print(TIME)
    main(SPARK, FILE, DATE, HOUR, TIME)
