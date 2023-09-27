# Reddit Averages ReadMe
reddit_averages.py takes a directory of reddit comments in .json format, then computes the average comment score for each subreddit. 

## Steps:
1) Convert each line of text in .json format to a python dict
2) Extract the subreddit and score from each dict
3) reduceByKey while summing to compute the count and sum_score for each subreddit
4) Compute sum_score/count for each key (subreddit)
5) Convert each tuple back to json format by mapping over each line using json.dumps

## To Submit Locally:
spark-submit reddit_averages2.py output-*

## To read Output From All Files:
cat output-1/part* | less