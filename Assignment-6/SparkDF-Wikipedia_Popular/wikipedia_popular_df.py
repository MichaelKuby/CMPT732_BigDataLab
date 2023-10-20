import sys, time
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types


wiki_schema = types.StructType([
    types.StructField('language', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('views', types.LongType()),
    types.StructField('bytes_transferred', types.LongType())
])


@functions.udf(returnType=types.StringType())
def path_to_hour(path):
    string_tokens = path.split('/') # Split the filename around '/'
    last_token = string_tokens[-1]
    strings = last_token.split('-') # Split the last token around '-'
    return strings[1] + "-" + strings[2][:2]


def main(inputs, output):
    wikiData = spark.read.csv(inputs, schema=wiki_schema, sep=' ')\
        .withColumn('filename', functions.input_file_name())
    wikiData = wikiData.withColumn('hour', path_to_hour(wikiData['filename']))
    wikiData = wikiData.drop('bytes_transferred', 'filename')\
        .filter(wikiData['language'] == functions.lit('en'))\
        .filter(~wikiData['title'].startswith('Special:'))\
        .filter(wikiData['title'] != functions.lit('Main_page'))

    wikiData = wikiData.cache()    # Cache after applying UDF and filtering

    mostViewed = wikiData.groupBy(wikiData['hour']).agg(functions.max(wikiData['views']).alias(
        'max_views'))

    wikiData_withMaxes = wikiData.join(mostViewed, 'hour')
    wikiDataMaxOnly = wikiData_withMaxes.filter(wikiData_withMaxes['views'] == wikiData_withMaxes['max_views']).drop(
        'max_views', 'language')

    results = wikiDataMaxOnly.sort(functions.asc('hour'))
    results.explain()
    results.write.json(output, mode='overwrite')


if __name__ == '__main__':
    # Spark DataFrame set-up
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
