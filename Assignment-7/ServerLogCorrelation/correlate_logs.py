import sys
import re
import math

from pyspark.sql import SparkSession, functions as F, types

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

logs_schema = types.StructType([
    types.StructField('host_name', types.StringType()),
    types.StructField('date_time', types.StringType()),
    types.StructField('req_path', types.StringType()),
    types.StructField('bytes_transferred', types.LongType())
])


def parse_line(record):
    match = re.search(line_re, record)
    if match:
        return match.group(1), match.group(2), match.group(3), int(match.group(4))
    else:
        # Record will return None
        return


def main(inputs):
    logs = sc.textFile(inputs)
    parsed_logs = logs.map(parse_line).filter(lambda x: x is not None)  # Remove None records
    logs_df = spark.createDataFrame(parsed_logs, logs_schema)

    # Grouping by host_name, aggregate to get the count of req_path and sum of bytes_transferred
    groups = logs_df.groupBy(logs_df['host_name']).agg(F.count(logs_df['req_path']),
                                                       F.sum(logs_df['bytes_transferred']))

    # Create a dataframe containing all the columns needed for the computation
    sums = groups.select(groups['host_name'], F.lit(1), groups['count(req_path)'], groups['count(req_path)'] ** 2,
                         groups['sum(bytes_transferred)'], groups['sum(bytes_transferred)'] ** 2,
                         groups['count(req_path)'] * groups['sum(bytes_transferred)'])

    sums.cache()    # Since we will use this many times

    # To compare our results against we can use the built-in function F.corr()
    # built_in_r = groups.agg(F.corr(groups['count(req_path)'], \
    #                                groups['sum(bytes_transferred)']) \
    #                         .alias('correlation')) \
    #     .first()['correlation']
    # print(built_in_r)

    # Do the computation of r
    n = sums.filter(sums['1'].isNotNull()).count()
    sum_x = sums.agg(F.sum(sums['count(req_path)']).alias('sum_x')).first()['sum_x']
    sum_xx = sums.agg(F.sum(sums['POWER(count(req_path), 2)']).alias('sum_xx')).first()['sum_xx']
    sum_y = sums.agg(F.sum(sums['sum(bytes_transferred)']).alias('sum_y')).first()['sum_y']
    sum_yy = sums.agg(F.sum(sums['POWER(sum(bytes_transferred), 2)']).alias('sum_yy')).first()['sum_yy']
    sum_xy = sums.agg(F.sum(sums['(count(req_path) * sum(bytes_transferred))']).alias('sum_xy')).first()['sum_xy']
    num = (n * sum_xy) - (sum_x * sum_y)
    denom = math.sqrt((n * sum_xx) - (sum_x ** 2)) * math.sqrt((n * sum_yy) - (sum_y ** 2))
    r = num / denom
    r2 = r ** 2
    print(f"r = {r:.6f}")
    print(f"r^2 = {r2:.6f}")


if __name__ == '__main__':
    input_arg = sys.argv[1]
    spark = SparkSession.builder.appName('correlate_logs.py').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input_arg)
