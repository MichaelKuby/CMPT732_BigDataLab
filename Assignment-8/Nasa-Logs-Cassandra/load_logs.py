import gzip
import os
import sys
import re

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

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


def main(input_directory, keyspace, table_name):
    # Connect to the cluster
    cluster = Cluster(['node1.local', 'node2.local'])
    session = cluster.connect(keyspace)

    # Prepare for a batch insert into Cassandra
    insert_records = session.prepare(f"INSERT INTO {table_name} (host, id, bytes, datetime, path) "
                                     f"VALUES (?, uuid(), ?, ?, ?)")
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

    # Manually retrieve input from the input directory
    counter = 0
    for f in os.listdir(input_directory):
        with gzip.open(os.path.join(input_directory, f), 'rt', encoding='utf-8') as logfile:
            for line in logfile:
                parsed = parse_line(line)
                if parsed:  # remove records that are None
                    host_name, date_time, req_path, bytes_transferred = parsed
                    date_time_obj = convert_to_datetime(date_time)
                    batch.add(insert_records, (host_name, bytes_transferred, date_time_obj, req_path))
                    counter += 1
                    if counter >= 250:  # Package inserts should have a few hundred lines
                        session.execute(batch)
                        batch.clear()
                        counter = 0
    session.execute(batch)  # Finalize the batch insert for all remaining lines
    batch.clear()


if __name__ == '__main__':
    input_directory = sys.argv[1]
    output_keyspace = sys.argv[2]
    table_name = sys.argv[3]
    main(input_directory, output_keyspace, table_name)
