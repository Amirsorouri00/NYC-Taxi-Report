# -*- coding: utf-8 -*-
#!/usr/bin/env python
# Imports
import sys
import timeit
from datetime import datetime
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
    # print(date)
    if date is None:
        return 0
    return date.weekday()


CALCULATE_DAY = udf(day_close, IntegerType())


def main(spark, file, name, time):
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
    # Data filtering to eliminate incorrect records
    # data = spark.sql("SELECT medallion, hack_license, pickup_datetime, dropoff_datetime, trip_time_in_secs, "
    #                  + "trip_distance, pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude"
    #                  + "FROM unprocessed "
    #                  + "WHERE medallion <> '' AND hack_license <> '' AND dropoff_datetime <> dropoff_datetime "
    #                  + "AND trip_time_in_secs > 0 "
    #                  + "AND pickup_longitude <> dropoff_longitude AND pickup_latitude <> dropoff_latitude ")
    # print("inja")
    #  + "AND (tipo_pago = 'CSH' OR tipo_pago = 'CRD')")
    # We filter the latitudes to eliminate invalid records
    # data = data.where(data.pickup_longitude >= INITIAL_LONGITUDE) \
    #     .where(data.pickup_longitude <= FINAL_LONGITUDE) \
    #     .where(data.dropoff_longitude >= INITIAL_LONGITUDE) \
    #     .where(data.dropoff_longitude <= FINAL_LONGITUDE) \
    #     .where(data.pickup_latitude >= FINAL_LATITUDE) \
    #     .where(data.pickup_latitude <= INITIAL_LATITUDE) \
    #     .where(data.dropoff_latitude >= FINAL_LATITUDE) \
    #     .where(data.dropoff_latitude <= INITIAL_LATITUDE)
    # print("oonja")
    # We establish the grid system and calculate the day of the week
    # print(data.pickup_datetime)
    data = data.withColumn("cuad_pickup_latitude",
                           floor((INITIAL_LATITUDE - data.pickup_latitude)/LATITUDE) + 1) \
        .withColumn("cuad_pickup_longitude",
                    floor(abs(INITIAL_LONGITUDE - data.pickup_longitude)/LONGITUDE) + 1) \
        .withColumn("cuad_dropoff_latitude",
                    floor((INITIAL_LATITUDE - data.dropoff_latitude)/LATITUDE) + 1) \
        .withColumn("cuad_dropoff_longitude",
                    floor(abs(INITIAL_LONGITUDE - data.dropoff_longitude)/LONGITUDE) + 1) \
        .withColumn("day_of_week", CALCULATE_DAY(data["pickup_datetime"]))
    # print("tahesh")
    # .option("compression", "snappy") \
    data.write \
        .parquet("./../data/processed/" + name + "lab.parquet")
    fin = timeit.default_timer()
    file = open("./../data/results/" + time + ".txt", "a")
    file.write(str(fin - beginning) + "\n")
    file.close()


if __name__ == "__main__":
    # Let's Configure SparkConf
    CONF = SparkConf()
    CONF.setAppName(APP_NAME)
    CONF.setMaster("local[*]")
    SPARK = SparkSession.builder.config(conf=CONF).getOrCreate()
    FILE = sys.argv[1]
    NAME = sys.argv[2]
    TIME = sys.argv[3]
    print(TIME)
    main(SPARK, FILE, NAME, TIME)
