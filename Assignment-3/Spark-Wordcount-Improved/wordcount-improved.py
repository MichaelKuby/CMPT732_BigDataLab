import sys
import re
import string

from pyspark import SparkConf, SparkContext

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


# add more functions as necessary

def words_once(line):
    wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))
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

def main(inputs, output):
    text = sc.textFile(inputs)
    text_repartitioned = text.repartition(80)
    words = text_repartitioned.flatMap(words_once)
    words_filtered = words.filter(remove_whitespace)
    wordcount = words_filtered.reduceByKey(add)

    outdata = wordcount.sortBy(get_key).map(output_format)
    outdata.saveAsTextFile(output)


# main logic starts here

if __name__ == '__main__':
    # Spark RDD Set-up
    conf = SparkConf().setAppName('wordcount-improved')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)