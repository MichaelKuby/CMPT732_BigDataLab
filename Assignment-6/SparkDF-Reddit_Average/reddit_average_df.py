import sys
from pyspark.sql import SparkSession, functions, types
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


comments_schema = types.StructType([
    types.StructField('archived', types.BooleanType()),
    types.StructField('author', types.StringType()),
    types.StructField('author_flair_css_class', types.StringType()),
    types.StructField('author_flair_text', types.StringType()),
    types.StructField('body', types.StringType()),
    types.StructField('controversiality', types.LongType()),
    types.StructField('created_utc', types.StringType()),
    types.StructField('distinguished', types.StringType()),
    types.StructField('downs', types.LongType()),
    types.StructField('edited', types.StringType()),
    types.StructField('gilded', types.LongType()),
    types.StructField('id', types.StringType()),
    types.StructField('link_id', types.StringType()),
    types.StructField('name', types.StringType()),
    types.StructField('parent_id', types.StringType()),
    types.StructField('retrieved_on', types.LongType()),
    types.StructField('score', types.LongType()),
    types.StructField('score_hidden', types.BooleanType()),
    types.StructField('subreddit', types.StringType()),
    types.StructField('subreddit_id', types.StringType()),
    types.StructField('ups', types.LongType()),
    # types.StructField('year', types.IntegerType()),
    # types.StructField('month', types.IntegerType()),
])


def main(inputs, output):
    comments = spark.read.json(inputs, schema=comments_schema)
    averages = comments.groupby('subreddit').avg('score')
    averages.explain()
    #averages.write.csv(output, mode='overwrite')


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
