from pyspark import SparkConf, SparkContext
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


def relative_scores(kv):
    subreddit, (subreddit_avg, dictionary) = kv
    author = dictionary['author']
    score = dictionary['score']
    return score / subreddit_avg, author


def main(inputs, output):
    # Read the data in as a text file
    in_RDD = sc.textFile(inputs)

    # Convert each line of text into a python dict using json.loads
    all_reddit_comments = in_RDD.map(json.loads).cache()

    # Form an RDD with kv = (subreddit, subreddit_avg_score)
    subreddit_avgs = all_reddit_comments\
        .map(get_fields)\
        .reduceByKey(add_pairwise)\
        .map(calculate_average)\
        .filter(remove_neg_avg)


    # Form an RDD with kv = (subreddits, comment data in dict form)
    comment_by_sub = all_reddit_comments.map(lambda c: (c['subreddit'], c))

    # Join across the two RDD's, returning (k, (v1, v2)) where (k, v1) is in subreddit_avgs,
    # and (k, v2) is in comment_by_sub.
    authors_relScores = subreddit_avgs.join(comment_by_sub)\
        .map(relative_scores)\
        .sortBy(lambda tuple: tuple[0], ascending=False)

    # Save to output
    authors_relScores.saveAsTextFile(output)


if __name__ == '__main__':
    # Spark RDD Set-up
    conf = SparkConf().setAppName('reddit averages')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
