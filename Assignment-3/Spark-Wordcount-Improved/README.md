# README for wordcount-improved.py

## Run Locally:
spark-submit wordcount_improved.py <input_folder> <output-folder>
spark-submit wordcount_improved.py wordcount-1 outputimproved-1
spark-submit wordcount_improved.py wordcount-2 outputimproved-2

## View the output:
less outputimproved-1/part*
less outputimproved-2/part*

# Improving wordcount
### Original runtime:

real	4m6.741s \
user	0m28.530s \
sys	0m3.111s

### Input Files:
The inputs are split between 8 different files. This is obviously suboptimal in the sense that we have 8 executors, 
so we are only guaranteeing parallelism upon initiation. But if one file is significantly larger than one of the 
others, it may be the bottleneck, taking a long time to finish.

#### Solution:
We should request that more partitions are created when loading the input. \

Having made the change minPartitions=80 I see the same results: \
\
real	4m9.658s \
user	0m25.961s \
sys	0m3.081s \
\
Having made the change I see the desired output:

real	2m21.592s \
user	0m34.398s \
sys	0m3.718s