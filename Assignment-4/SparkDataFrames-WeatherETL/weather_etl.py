import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types


def main(inputs, output):
    # Configure the schema to be used for the dataframe.
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

    # Read csv files into the dataframe. These are partitioned subsets of the original GHCN data
    weather = spark.read.csv(inputs, schema=observation_schema)

    # Filter out observations where the qflat is set to something other than Null
    weather = weather.filter(weather['qflag'].isNull())

    # Keep only records from stations that start with 'CA' (Canadian data)
    weather = weather.filter(weather['station'].startswith('CA'))

    # Keep only maximum temperature observations
    weather = weather.filter(weather['observation'] == 'TMAX')

    # Divide the temperature by 10 to put it into C
    weather = weather.withColumn('tmax', weather['value'] / 10)

    # Keep only the columns we want
    weather = weather.select('station', 'date', 'tmax')

    # Write the results as a directory of JSON files GZIp compressed (in the Spark one-JSON-object-per-line way)
    weather.write.json(output, compression='gzip', mode='overwrite')


# main logic starts here

if __name__ == '__main__':
    # Spark DataFrame set-up
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Weather ETL').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
