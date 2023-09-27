from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

import json
# add more functions as necessary

def loads(jsonLine):
    # Converts a line of text in .json format to a python dict
    return json.loads(jsonLine)

def get_fields(dict):
    # Extracts the subreddit and score from a dict
    subreddit = dict['subreddit']
    score = int(dict['score'])
    return subreddit, (1, score)

def add_pairwise(tuple1, tuple2):
    num1, score1 = tuple1
    num2, score2 = tuple2
    return (num1 + num2, score1 + score2)

def calculate_average(triple):
    # Computes the average for each subreddit, returning a tuple (subreddit, avg_score)
    subreddit, (count, sum_score) = triple
    return subreddit, sum_score / count

def main(inputs, output):
    # Load the file in as a text file
    RDD = sc.textFile(inputs)

    # Apply json.loads and get_fields to each record to extract what we need
    reddit_comments = RDD.map(loads).map(get_fields).reduceByKey(add_pairwise)
    subreddit_avg = reddit_comments.map(calculate_average)
    results = subreddit_avg.map(json.dumps)
    results.saveAsTextFile(output)

if __name__ == '__main__':
    # Spark RDD Set-up
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)