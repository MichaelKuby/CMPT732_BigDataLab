from pyspark import SparkConf, SparkContext
import sys
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('word count')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+


def words_once(line):
    stripped_lines = wordsep.split(line.lower())
    for w in stripped_lines:
        yield (w, 1)


def add(x, y):
    return x + y


def get_key(kv):
    return kv[0]


def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)


def remove_whitespace(kv):
    k, v = kv
    return not k.isspace() and k != ""


text = sc.textFile(inputs)
words = text.flatMap(words_once)
words_filtered = words.filter(remove_whitespace)
wordcount = words_filtered.reduceByKey(add)

outdata = wordcount.sortBy(get_key).map(output_format)
outdata.saveAsTextFile(output)
