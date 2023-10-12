# README for weather_etl.py

## Objective: 
Learn how to use pyspark to read data from a CSV directly into a spark Dataframe

## Run code using:
spark-submit weather_etl.py weather-1 output

## View the output using:
cat output/part-0000* | zless
