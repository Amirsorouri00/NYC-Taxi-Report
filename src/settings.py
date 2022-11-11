# -*- coding: utf-8 -*-
#!/usr/bin/env python
"""
Establishes the global variables used in this project

This file contains data which is inmutable used for solving the
problem, like the size of the map and the cells.
"""
from datetime import datetime

LATITUDE = 0.004491556
LONGITUDE = 0.005986
MAPSIZE = 300
INITIAL_LATITUDE = 41.477182778
INITIAL_LONGITUDE = -74.916578
FINAL_LATITUDE = 40.129715978
FINAL_LONGITUDE = -73.120778


def get_timestamp(date, hour):
    """
    method to get a timestamp with a date and a time
    :param date: Desired date in format YY-MM-DD
    :param hour: Desired time in format HH:MM
    :return: timestamp with the entered date and time
    """
    return datetime.strptime(date + " " + hour + ":00", "%Y-%m-%d %H:%M:%S")


def get_day_of_week(day):
    """
    Method to get the day of the week in int format for
    be able to use it with system data. where 0 is the
    Monday and 6 on Sunday.
    :param day: day of the week written in a string
    :return: day of the week in int format
    """
    day = day.lower()
    day_number = 0
    if day == "lunes":
        day_number = 0
    elif day == "martes":
        day_number = 1
    elif day == "miercoles" or day == "miércoles":
        day_number = 2
    elif day == "jueves":
        day_number = 3
    elif day == "viernes":
        day_number = 4
    elif day == "sabado" or day == "sábado":
        day_number = 5
    elif day == "domingo":
        day_number = 6
    else:
        print("You have not set a day of the week, by default it is Monday")
        day_number = 0
    return day_number


def get_month(month):
    """
    Method to obtain the month of the week in int format for
    be able to use it with system data. where 1 is january
    and December 12.
    :param mes: month of the year written in a string
    :return: month of the year written in int format
    """
    month = month.lower()
    month_number = "01"
    if month == "enero":
        month_number = "01"
    elif month == "febrero":
        month_number = "02"
    elif month == "marzo":
        month_number = "03"
    elif month == "abril":
        month_number = "04"
    elif month == "mayo":
        month_number = "05"
    elif month == "junio":
        month_number = "06"
    elif month == "julio":
        month_number = "07"
    elif month == "agosto":
        month_number = "08"
    elif month == "septiembre":
        month_number = "09"
    elif month == "octubre":
        month_number = "10"
    elif month == "noviembre":
        month_number = "11"
    elif month == "diciembre":
        month_number = "12"
    else:
        print("You have not set a month, default is January")
    return month_number
