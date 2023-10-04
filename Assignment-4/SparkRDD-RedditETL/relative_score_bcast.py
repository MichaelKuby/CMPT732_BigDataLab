from pyspark import SparkConf, SparkContext
from typing import List, Dict, Union
import sys
import json

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def get_fields(dict):
    # Extracts the subreddit and score from a dict
    subreddit = dict['subreddit']
    score = dict['score']
    return subreddit, (1, score)


def add_pairwise(record1, record2):
    num1, score1 = record1
    num2, score2 = record2
    return (num1 + num2, score1 + score2)


def calculate_average(record):
    # Computes the average for each subreddit, returning a tuple (subreddit, avg_score)
    subreddit, (count, sum_score) = record
    return subreddit, sum_score / count


def remove_neg_avg(record):
    subreddit, avg = record
    if avg > 0:
        return True
    return False


def relative_score(bc, comment: Dict):
    author = comment['author']
    score = comment['score']
    subreddit = comment['subreddit']
    average = bc.value[subreddit]
    return score/average, author


def main(inputs, output):
    # Read the data in as a text file
    in_RDD = sc.textFile(inputs)

    # Convert each line of text into a python dict
    all_reddit_comments = in_RDD.map(json.loads).cache()

    # Form an RDD with kv = (subreddit, subreddit_avg_score)
    subreddit_avgs = all_reddit_comments\
        .map(get_fields)\
        .reduceByKey(add_pairwise)\
        .map(calculate_average)\
        .filter(remove_neg_avg)

    # By outputting to a file I can see there are only 5 records in subreddit_avgs after reduceByKey on input =
    # reddit-4. Join will cause a shuffle operation which is unnecessary.
    # Code: subreddit_avgs.saveAsTextFile(output + '/averages')

    # Instead, let's turn it into a dict and wrap in a broadcast object that we can pass around.
    bc = sc.broadcast(dict(subreddit_avgs.collect()))

    # Map over all_reddit_comments to compute a new RDD of (relative_score, author) records.
    # Sort in descending ord.
    relScores_and_authors = all_reddit_comments\
        .map(lambda c: relative_score(bc, c))\
        .sortBy(lambda t: t[0], ascending=False)

    # Save output
    relScores_and_authors.saveAsTextFile(output)


if __name__ == '__main__':
    # Spark RDD Set-up
    conf = SparkConf().setAppName('relative score broadcast')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
