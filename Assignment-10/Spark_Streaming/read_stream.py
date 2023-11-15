import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from kafka import KafkaConsumer

from pyspark.sql import SparkSession, functions


def main(topic):
    messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', 'node1.local:9092,node2.local:9092') \
        .option('subscribe', topic).load()

    values = messages.select(functions.col('value').cast('string'))
    values_xy = values.select(functions.split(values['value'], " ").alias('split_val'))
    values_xy = values_xy.select(functions.col('split_val').getItem(0).cast('int').alias('x'),
                                 functions.col('split_val').getItem(1).cast('int').alias('y'))

    sums = values_xy.groupBy().agg(
        functions.sum('x').alias('sum_x'),
        functions.sum('y').alias('sum_y'),
        functions.sum(values_xy['x'] * values_xy['y']).alias('sum_xy'),
        functions.sum(values_xy['x'] * values_xy['x']).alias('sum_xx'),
        functions.count('x').alias('N')
    )

    # Calculate Beta
    beta = (sums['sum_xy'] - ((sums['sum_x'] * sums['sum_y']) / sums['N'])) / \
           (sums['sum_xx'] - (sums['sum_x'] ** 2 / sums['N']))

    # Calculate Alpha
    alpha = (sums['sum_y'] / sums['N']) - (beta * sums['sum_x'] / sums['N'])

    # Create results DataFrame with both Beta and Alpha
    results = sums.select(beta.alias('Beta'), alpha.alias('Alpha'))

    stream = results.writeStream.format('console') \
        .outputMode('update').start()
    stream.awaitTermination(600)


if __name__ == '__main__':
    topic = sys.argv[1]
    spark = SparkSession.builder.appName('read_stream').getOrCreate()
    assert spark.version >= '3.0'
    spark.sparkContext.setLogLevel('WARN')
    main(topic)
