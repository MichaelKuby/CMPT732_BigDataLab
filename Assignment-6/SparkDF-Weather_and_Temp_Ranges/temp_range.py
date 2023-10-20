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


# add more functions as necessary

def main(inputs, output):
    weather_data = spark.read.csv(inputs, schema=observation_schema)
    weather_data = weather_data.where(weather_data['qflag'].isNull())
    weather_data = weather_data.withColumn('value', weather_data['value'] / 10)     # DataFrames are immutable, which is why we use withColumn

    weather_data.cache()

    weather_max = weather_data.select('date', 'station', 'value').where(weather_data['observation'] == 'TMAX').withColumnRenamed('value', 'TMAX')
    weather_min = weather_data.select('date', 'station', 'value').where(weather_data['observation'] == 'TMIN').withColumnRenamed('value', 'TMIN')
    weather_max_min = weather_max.join(weather_min, ['date', 'station'])
    weather_range = weather_max_min.withColumn('range', F.round(F.abs(weather_max_min['TMAX'] - weather_max_min['TMIN']), 1))

    weather_range.cache()

    weather_max_range = weather_range.groupBy(weather_range['date']).max().select('date', 'max(range)')
    weather_joined = weather_range.join(weather_max_range, 'date')
    weather_joined = weather_joined.where(weather_joined['range'] == weather_joined['max(range)']).select('date', 'station', 'range')
    weather_results = weather_joined.sort(['date', 'station'])
    weather_results.show()
    weather_results.write.csv(output, mode='overwrite')



if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
