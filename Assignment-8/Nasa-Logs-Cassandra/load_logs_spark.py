import sys
import re

from pyspark.sql import SparkSession, functions as F, types
from datetime import datetime

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def parse_line(record):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    match = re.search(line_re, record)
    if match:
        return match.group(1), match.group(2), match.group(3), int(match.group(4))
    else:
        # Record will return None
        return


def convert_to_datetime(date_time_str):
    # Define the expected pattern
    pattern = re.compile(r'\d{2}/[a-zA-Z]{3}/\d{4}:\d{2}:\d{2}:\d{2}')

    # Search for the pattern in date_time_str
    match = pattern.search(date_time_str)

    if match:
        extracted_str = match.group(0)
        try:
            # Convert the extracted string to a datetime object
            date_time_obj = datetime.strptime(extracted_str, "%d/%b/%Y:%H:%M:%S")
            return date_time_obj
        except ValueError:
            print(f"Warning: Failed to convert date-time: {extracted_str}")
            return datetime(1970, 1, 1, 0, 0, 0)  # Return the start of Unix time as a sentinel value
    else:
        print(f"Warning: Invalid date-time format: {date_time_str}")
        return datetime(1970, 1, 1, 0, 0, 0)  # Return the start of Unix time as a sentinel value


def main(input_directory, output_keyspace, table_name):
    # Schema for Spark Dataframe
    logs_schema = types.StructType([
        types.StructField('host', types.StringType()),
        types.StructField('datetime', types.StringType()),
        types.StructField('path', types.StringType()),
        types.StructField('bytes', types.LongType())
    ])

    logs = sc.textFile(input_directory)
    parsed_logs = logs.map(parse_line).filter(lambda x: x is not None)  # Remove None records
    logs_df = spark.createDataFrame(parsed_logs, logs_schema)

    # Create and apply UDF to use my string to datetime object conversion
    convert_to_datetime_udf = F.udf(convert_to_datetime, types.TimestampType())
    logs_df = logs_df.withColumn("datetime", convert_to_datetime_udf('datetime'))

    # Add an id column with uuid function
    logs_df = logs_df.withColumn('id', F.expr('uuid()'))

    # Repartition to control the degree of parallelism
    logs_df = logs_df.repartition(80)   # /W repartitioning: 4 min 35 sec;  /WO: 6 min 7 sec

    # Write DataFrame to Cassandra
    logs_df.write.format("org.apache.spark.sql.cassandra")\
        .options(table=table_name, keyspace=output_keyspace) \
        .mode('Append')\
        .save()


if __name__ == '__main__':
    # Parameter set-up
    input_directory = sys.argv[1]
    output_keyspace = sys.argv[2]
    table_name = sys.argv[3]
    # Configure the SparkSession object to connect correctly to SFU's cluster
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('load_logs_spark_cassandra')\
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input_directory, output_keyspace, table_name)
