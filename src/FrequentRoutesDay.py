# -*- coding: utf-8 -*-
#!/usr/bin/env python
# Imports
import sys
import timeit
from datetime import datetime, timedelta

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, udf
from pyspark.sql.types import BooleanType, IntegerType

from settings import get_day_of_week, get_month

# Constantes
APP_NAME = "Most frequent routes given a day"


# Variables globales
FILE = sys.argv[1]
MONTH = get_month(sys.argv[2])
WEEK_DAY = sys.argv[3]
HOUR = sys.argv[4]
END_HOUR = datetime.strptime("2013-" + MONTH + " " + HOUR, "%Y-%m %H:%M")
START_HOUR = END_HOUR - timedelta(minutes=30)


def compare_time(hour):
    """
        Method that filters the times of the records so that they match
        with the desired search hours
        :param hour: Full timestamp
        :return: True if the timestamp hours are between the desired ones
         false if otherwise
    """
    if hour.time() <= END_HOUR.time() and hour.time() >= START_HOUR.time():
        return True
    return False


def relevance(file):
    """
        Method that gives more relevance to trips closest to the
         desired search date.
        If the difference is less than one month from the date
         given the records have more relevance
        :param file: Full timestamp
        :return: 2 if the trip is close to the desired date, 1 if not
    """
    diferencia = file - END_HOUR
    if diferencia < timedelta(days=30) and diferencia > timedelta(days=-30):
        return 2
    else:
        return 1


check_time = udf(compare_time, BooleanType())
calculate_relevance = udf(relevance, IntegerType())


def main(spark, file):
    """
    Calculation of the most frequent routes given a month, a day of the week and
     one hour within the entire data set. The nearest trips
     the month entered will have more relevance
    :param spark: Spark instance
    :param file: Data file
    :return: Ten most frequent routes
    """
    beginning = timeit.default_timer()

    # data = spark.read.format("parquet").load("./../data/processed/" + file)
    file_path_list = ["./../data/processed/1lab.parquet",
                      "./../data/processed/2lab.parquet",
                      "./../data/processed/3lab.parquet",
                      "./../data/processed/4lab.parquet",
                      "./../data/processed/5lab.parquet",
                      "./../data/processed/6lab.parquet",
                      "./../data/processed/7lab.parquet",
                      "./../data/processed/8lab.parquet",
                      "./../data/processed/9lab.parquet",
                      "./../data/processed/10lab.parquet",
                      "./../data/processed/11lab.parquet",
                      "./../data/processed/12lab.parquet",
                      ]

    data = spark.read.parquet(*file_path_list)
    chosen_day = get_day_of_week(WEEK_DAY)

    """
    Filtramos los datos con respecto al dia de la semana y la hora
    Ademas le damos un relevancia a cada viaje para el posterior count
    """
    filtered = data.filter(data.day_of_week == chosen_day) \
        .withColumn("joder", check_time(data.pickup_datetime)) \
        .withColumn("joder2", check_time(data.dropoff_datetime)) \
        .withColumn('relevancia', calculate_relevance(data.pickup_datetime))
    """
    Agrupamos por rutas y hacemos el recuento de viajes
    """
    frequent = filtered.groupBy("cuad_pickup_longitude", "cuad_pickup_latitude",
                                "cuad_dropoff_longitude", "cuad_dropoff_latitude") \
        .sum("relevancia") \
        .select(col("cuad_pickup_longitude"), col("cuad_pickup_latitude"),
                col("cuad_dropoff_longitude"), col("cuad_dropoff_latitude"),
                col("sum(relevancia)").alias("frecuencia")) \
        .orderBy("frecuencia", ascending=False)

    final = frequent.take(10)

    fin = timeit.default_timer()
    file = open("./../data/results/" + "frequentDayResults.txt", "a")
    file.write(str(START_HOUR.time()) + ", " + str(END_HOUR.time()) + ", ")
    for i in range(len(final)):
        file.write(str(i) + ": ")
        file.write("(" + str(final[i][0]) + ", " + str(final[i][1]) + ") ")
        file.write("(" + str(final[i][2]) + ", " + str(final[i][3]) + "), ")
    file.write(str(fin - beginning) + "\n")
    file.close()


if __name__ == "__main__":
    # Configuramos SparkConf
    CONF = SparkConf()
    CONF.setAppName(APP_NAME)
    CONF.setMaster("local[*]")
    SPARK = SparkSession.builder.config(conf=CONF).getOrCreate()
    main(SPARK, FILE)
