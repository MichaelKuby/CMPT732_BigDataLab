from pyspark import SparkConf, SparkContext
import sys
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('word count')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+


def line_to_tuple(line):
    stripped_lines = line.split()
    date_time, lang, page_name, requests, bytes_transferred = stripped_lines
    requests = int(requests)
    return date_time, lang, page_name, requests, bytes_transferred


def remove_main_and_special(this_tuple):
    date_time, lang, page_name, requests, bytes_transferred = this_tuple
    return lang == "en" and not (page_name == "Main_Page" or page_name.startswith("Special:"))


def tuple_to_kv(this_tuple):
    date_time, lang, page_name, requests, bytes_transferred = this_tuple
    return date_time, (requests, page_name)


def maximum(rp1, rp2):
    r1, p1 = rp1
    r2, p2 = rp2
    if r1 > r2:
        return rp1
    else:
        return rp2


def get_key(kv):
    return kv[0]


def tab_separated(kv):
    return "%s\t%s" % (kv[0], kv[1])


# Read the input files in as lines
text = sc.textFile(inputs)

# Break each line up into tuples
tuples = text.map(line_to_tuple)

# Remove the records we don't want to consider
tuples_filtered = tuples.filter(remove_main_and_special)

# Create an RDD of key-value pairs
date_and_views = tuples_filtered.map(tuple_to_kv)

# Reduce to find the max value for each key
date_and_views_max = date_and_views.reduceByKey(maximum)

# Sort so the records are sorted by key
date_and_views_max_sorted = date_and_views_max.sortBy(get_key)

# Save as text output analogous to MapReduce
date_and_views_max_sorted.map(tab_separated).saveAsTextFile(output)

# Check what's going on in the RDD
print(date_and_views_max_sorted.take(10))