import sys
import math

from pyspark.sql import SparkSession, functions as F

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def main(keyspace, table_name):

    # Load in data from the Cassandra Database located at SFU
    logs = spark.read.format("org.apache.spark.sql.cassandra").options(table=table_name, keyspace=keyspace).load()

    # Grouping by host_name, aggregate to get the count of req_path and sum of bytes_transferred
    groups = logs.groupBy(logs['host']).agg(F.count(logs['path']),
                                                       F.sum(logs['bytes']))

    # Create a dataframe containing all the columns needed for the computation
    sums = groups.select(groups['host'], F.lit(1), groups['count(path)'], groups['count(path)'] ** 2,
                         groups['sum(bytes)'], groups['sum(bytes)'] ** 2,
                         groups['count(path)'] * groups['sum(bytes)'])

    sums.cache()  # Since we will use this many times

    # Do the computation of r
    n = sums.filter(sums['1'].isNotNull()).count()
    sum_x = sums.agg(F.sum(sums['count(path)']).alias('sum_x')).first()['sum_x']
    sum_xx = sums.agg(F.sum(sums['POWER(count(path), 2)']).alias('sum_xx')).first()['sum_xx']
    sum_y = sums.agg(F.sum(sums['sum(bytes)']).alias('sum_y')).first()['sum_y']
    sum_yy = sums.agg(F.sum(sums['POWER(sum(bytes), 2)']).alias('sum_yy')).first()['sum_yy']
    sum_xy = sums.agg(F.sum(sums['(count(path) * sum(bytes))']).alias('sum_xy')).first()['sum_xy']
    num = (n * sum_xy) - (sum_x * sum_y)
    denom = math.sqrt((n * sum_xx) - (sum_x ** 2)) * math.sqrt((n * sum_yy) - (sum_y ** 2))
    r = num / denom
    r2 = r ** 2
    print(f"r = {r:.6f}")
    print(f"r^2 = {r2:.6f}")


if __name__ == '__main__':
    # Parameter set-up
    keyspace = sys.argv[1]
    table_name = sys.argv[2]

    # Configure the SparkSession object to connect correctly to SFU's cluster
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('load_logs_spark_cassandra')\
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(keyspace, table_name)
