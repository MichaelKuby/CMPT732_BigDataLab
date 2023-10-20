import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions as F, types


observation_schema = types.StructType([
        types.StructField('station', types.StringType()),
        types.StructField('date', types.StringType()),
        types.StructField('observation', types.StringType()),
        types.StructField('value', types.IntegerType()),
        types.StructField('mflag', types.StringType()),
        types.StructField('qflag', types.StringType()),
        types.StructField('sflag', types.StringType()),
        types.StructField('obstime', types.StringType()),
    ])


def main(inputs, output):
    weather_data = spark.read.csv(inputs, schema=observation_schema)
    weather_data.createOrReplaceTempView("weather")

    spark.sql("SELECT * FROM weather WHERE qflag IS NULL").createOrReplaceTempView("weather")

    weather = spark.sql("SELECT station, date, observation, mflag, qflag, sflag, obstime, value / 10 AS value FROM weather")

    weather.cache()
    weather.createOrReplaceTempView("weather")

    spark.sql("SELECT date, station, value AS TMAX FROM weather WHERE observation = 'TMAX'").createOrReplaceTempView("tmax")
    spark.sql("SELECT date, station, value AS TMIN FROM weather WHERE observation = 'TMIN'").createOrReplaceTempView("tmin")

    spark.sql("SELECT tmax.*, tmin.TMIN FROM tmax INNER JOIN tmin ON tmax.date = tmin.date AND tmax.station = tmin.station").createOrReplaceTempView("tMinMax")
    tRange = spark.sql("SELECT *, round(abs(TMAX - TMIN), 1) AS range FROM tMinMax")

    tRange.cache()
    tRange.createOrReplaceTempView("tRange")

    spark.sql("SELECT date, max(range) AS max_range FROM tRange GROUP BY date").createOrReplaceTempView("maxRange")
    spark.sql("SELECT tRange.*, maxRange.max_range FROM tRange INNER JOIN maxRange ON tRange.date = maxRange.date").createOrReplaceTempView("joined")
    spark.sql("SELECT * FROM joined WHERE range = max_range").createOrReplaceTempView("results")
    spark.sql("SELECT * FROM results ORDER BY date, station ASC").createOrReplaceTempView("results")

    weather_results = spark.sql("SELECT date, station, range FROM results")
    weather_results.write.csv(output, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
