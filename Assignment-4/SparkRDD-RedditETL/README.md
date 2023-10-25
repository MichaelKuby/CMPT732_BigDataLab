# README for reddit_etl.py

## Objective:
Do a simple transformation on Reddit data, tidying up the data and keeping only the subset we need.

## Run using:
time spark-submit reddit_etl.py reddit-2 output

## .cache
times without: 
1) 17.35s user 1.54s system 176% cpu 10.678 total
2) 18.03s user 1.65s system 176% cpu 11.159 total
3) 16.50s user 1.51s system 182% cpu 9.861 total
4) 17.83s user 1.77s system 178% cpu 10.983 total

average total without cache: 10.670250000000001

time with:
1) 16.42s user 1.54s system 185% cpu 9.699 total
2) 17.11s user 1.55s system 184% cpu 10.121 total
3) 16.45s user 1.67s system 164% cpu 11.034 total
4) 17.59s user 1.62s system 180% cpu 10.641 total

average total with cache: 10.37375

Correction: Assume 4.5 second overhead for starting spark.

Average run times are then: \
average total without cache: 6.170250000000001 \
average total with cache: 5.873749999999999

Speedup: 1 - 5.873749999999999 / 6.170250000000001 = 0.048053158299907084

So we see a roughly 4.8% speedup.

# README for relative_score.py

## Objective: 
Figure out who made *The Best* comment on Reddit. I.e., find the comment with the highest score relative to the subreddit's average.

## Approach: 
1) Calculate the RDD of (subreddit, average scores)
2) Form a pair RDD of (subreddit, comment data in dict form)
3) Join on subreddit
4) Map to produce (relative_score, author) for each record
5) Sort by the relative score and output

## Run locally using: \
spark-submit relative_score.py reddit-2 output

## Run on the cluster using: \
spark-submit --conf spark.dynamicAllocation.enabled=false --num-executors=4 --executor-cores=4 --executor-memory=2g relative_score.py /courses/732/reddit-2 output
spark-submit --conf spark.dynamicAllocation.enabled=false --num-executors=4 --executor-cores=4 --executor-memory=2g relative_score.py /courses/732/reddit-4/ output

# README for relative_score_bcast.py

## Objective:
Speed up the run time of relative_score.py

## Approach:
1) recognize that after reducing by key we have a very small RDD
2) Turn that RDD into a broadcast object
3) Pass the broadcast object to each executor node to do the computation

## Run on the cluster using: \
spark-submit --conf spark.dynamicAllocation.enabled=false --num-executors=4 --executor-cores=4 --executor-memory=2g relative_score_bcast.py /courses/732/reddit-4/ output