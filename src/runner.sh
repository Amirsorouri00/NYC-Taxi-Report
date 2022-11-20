#!/bin/zsh
/opt/homebrew/bin/python3 DataProcessing.py trip_data_1.csv 1   $(date +%s)
/opt/homebrew/bin/python3 DataProcessing.py trip_data_2.csv 2   $(date +%s)
/opt/homebrew/bin/python3 DataProcessing.py trip_data_3.csv 3   $(date +%s)
/opt/homebrew/bin/python3 DataProcessing.py trip_data_4.csv 4   $(date +%s)
/opt/homebrew/bin/python3 DataProcessing.py trip_data_5.csv 5   $(date +%s)
/opt/homebrew/bin/python3 DataProcessing.py trip_data_6.csv 6   $(date +%s)
/opt/homebrew/bin/python3 DataProcessing.py trip_data_7.csv 7   $(date +%s)
/opt/homebrew/bin/python3 DataProcessing.py trip_data_8.csv 8   $(date +%s)
/opt/homebrew/bin/python3 DataProcessing.py trip_data_9.csv 9   $(date +%s)
/opt/homebrew/bin/python3 DataProcessing.py trip_data_10.csv 10 $(date +%s)
/opt/homebrew/bin/python3 DataProcessing.py trip_data_11.csv 11 $(date +%s)
/opt/homebrew/bin/python3 DataProcessing.py trip_data_12.csv 12 $(date +%s)
/opt/homebrew/bin/python3 FrequentRoutes.py 1lab.parquet 2013-02-01 01:00
opt/homebrew/bin/python3 FrequentRoutesDay.py x enero martes 11:00