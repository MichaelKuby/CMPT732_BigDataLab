# README for euler.py

We are estimating Euler's constant, e, using a stochastic representation. Given a sequence X_1, X_2, ... of uniform 
random variables on the range [0,1], we can take the smallest number of these such that the total is greater than one:

V = min {n | X_1 + X_2 + ... + X_n > 1}

Then the expected value of V is equal to e, 

E(V) = e.

## Approach:
1. Get the number of chosen samples from the command line
2. Create an RDD with sample many indices
3. For each index, sample to find V
4. Find the mean of all the V's

## Run using:
spark-submit euler.py 1000000000 \
Calling for 1000000000 samples.

## Finding the ideal number of partitions. Note: My CPU has 4 cores.

### 1 partition


### 2 partitions


### 4 partitions


### 16 partitions


### 60 partitions


### 120 partitions


### 240 partitions


## Running Tests to see the difference in times between different python run-time environments
Optimal parameters for this machine: \
num_batches = 100 \
numSlices = 40 \
spark-submit euler.py 1000000000  16.04s user 1.49s system 21% cpu 1:21.51 total

1) First: set the paths:
   2) PY3_PATH=$(pyenv which python3)
   3) PYPY_PATH=$(pyenv which pypy)

2) Standard CPython Implementation
   1) Run using: 
      1) export PYSPARK_PYTHON=$PY3_PATH
      2) time ${SPARK_HOME}/bin/spark-submit euler.py 1000000000
   2) Results: 
      1) 2.7182462739999997
      2) 17.40s user 1.66s system 19% cpu 1:37.32 total

2) Spark Python with PyPy
   1) Run using: 
      1) export PYSPARK_PYTHON=$PYPY_PATH 
      2) time ${SPARK_HOME}/bin/spark-submit euler.py 1000000000
   2) Results: 
      1) 2.718282213999999 
      2) 16.96s user 1.62s system 19% cpu 1:36.21 total

3) Non-Spark Single-Threaded PyPy
    1) Run using:
       1) time $PYPY_PATH euler_single.py 1000000000
    2) Results:
       1) 2.71831421
       2) 56.95s user 0.62s system 98% cpu 58.221 total

4) Non-Spark Single-Threaded C
    1) Run using:
       1) gcc -Wall -O2 -o euler euler.c && time ./euler 1000000000
    2) Results:
       1) 2.71828
       2) 28.45s user 0.06s system 98% cpu 28.959 total

# LAB RESULTS
num_batches = 10000 \
numSlices = 40 \
spark-submit euler.py 1000000000  18.12s user 1.53s system 22% cpu 1:27.87 total

num_batches = 10000 \
numSlices = 80 \
spark-submit euler.py 1000000000  18.39s user 1.61s system 23% cpu 1:25.04 total

num_batches = 10000 \
numSlices = 160 \
spark-submit euler.py 1000000000  22.71s user 2.08s system 24% cpu 1:40.56 total

num_batches = 1000 \
numSlices = 40 \
spark-submit euler.py 1000000000  16.75s user 1.82s system 20% cpu 1:30.52 total

num_batches = 1000 \
numSlices = 80 \
spark-submit euler.py 1000000000  19.09s user 1.65s system 23% cpu 1:29.35 total

num_batches = 1000 \
numSlices = 160 \
spark-submit euler.py 1000000000  19.72s user 1.84s system 24% cpu 1:28.36 total

num_batches = 100 \
numSlices = 40 \
spark-submit euler.py 1000000000  16.04s user 1.49s system 21% cpu 1:21.51 total

num_batches = 100 \
numSlices = 80 \
spark-submit euler.py 1000000000  17.79s user 1.48s system 20% cpu 1:32.61 total

num_batches = 10 \
numSlices = 160 \
spark-submit euler.py 1000000000  21.23s user 2.47s system 23% cpu 1:41.77 total
