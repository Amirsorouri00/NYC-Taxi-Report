# -*- coding: utf-8 -*-
#!/usr/bin/env python
# Imports
import sys
import timeit
from datetime import datetime, timedelta

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from settings import get_timestamp

# Constantes
APP_NAME = "Most frequent routes"


def main(spark, file, date, hour):
    """
    Calculation of the most frequent routes given a date and time
    within the entire data set.
    :param spark: Spark instance
    :param file: data file
    :param date: String with the search date in the form "YYYY-MM-DD".
    Ej: "2013-01-02"
    :param hour: Time on which you want to make the query in the form "HH:MM"
    :return: Ten most frequent routes
    """
    beginning = timeit.default_timer()
    data = spark.read.format("parquet").load("./../data/processed/" + file)
    data.show()
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
    # frequent.show()
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


if __name__ == "__main__":
    # Let's Configure SparkConf
    CONF = SparkConf()
    CONF.setAppName(APP_NAME)
    CONF.setMaster("local[*]")
    SPARK = SparkSession.builder.config(conf=CONF).getOrCreate()
    FILE = sys.argv[1]
    DATE = sys.argv[2]
    HOUR = sys.argv[3]
    main(SPARK, FILE, DATE, HOUR)
