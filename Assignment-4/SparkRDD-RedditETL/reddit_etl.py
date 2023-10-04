from pyspark import SparkConf, SparkContext
import sys
import json

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


def get_fields(dict):
    # Extracts the subreddit and score from a dict
    subreddit = dict['subreddit']
    score = int(dict['score'])
    author = dict['author']
    return subreddit, score, author


def keep_e(triple):
    subreddit, score, author = triple
    if 'e' in subreddit.lower():
        return True
    return False


def pos_score(triple):
    subreddit, score, author = triple
    if score > 0:
        return True
    return False


def non_pos_score(triple):
    subreddit, score, author = triple
    if score <= 0:
        return True
    return False


def main(inputs, output):
    # Read the data in as a text file
    in_RDD = sc.textFile(inputs)

    # Convert each line of text into a python dict
    reddit_comments = in_RDD.map(json.loads).map(get_fields).filter(keep_e)

    # Cache the RDD as we use it twice in the following steps
    reddit_comments.cache()

    # Create an RDD with all scores positive
    reddit_comments_pos = reddit_comments.filter(pos_score)

    # Create an RDD with all scores non-positive
    reddit_comments_nonpos = reddit_comments.filter(non_pos_score)

    # write each to their appropriate directories
    reddit_comments_pos.map(json.dumps).saveAsTextFile(output + '/positive')
    reddit_comments_nonpos.map(json.dumps).saveAsTextFile(output + '/negative')


if __name__ == '__main__':
    # Spark RDD Set-up
    conf = SparkConf().setAppName('reddit_etl')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
