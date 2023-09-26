from pyspark import SparkConf, SparkContext
import sys
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('word count')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))

# re.compile(...): This function compiles a regular expression pattern into a regular expression object, which can
# be used for matching using its match() and search() methods, among others.

# r'[%s\s]+': This is the regular expression pattern. The %s will be replaced by the escaped punctuation string,
# and \s matches any whitespace character. The + means that the pattern will match one or more of these
# characters in a row.

# re.escape(string.punctuation): The re.escape function escapes all special characters in string.punctuation.
# This makes sure that they are treated as literal characters in the regular expression. string.punctuation
# contains a string of ASCII characters which are considered punctuation characters.

# % re.escape(string.punctuation): This is using Python's old-style string formatting to replace the %s in
# the regular expression pattern with the escaped string of punctuation characters.


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
