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
spark-submit euler.py 1000000 \
Calling for 10000000 samples.

## Finding the ideal number of partitions. Note: My CPU has 4 cores.

### 1 partition
spark-submit euler.py 5000000  9.33s user 0.84s system 9% cpu 1:49.83 total

### 2 partitions
spark-submit euler.py 5000000  9.07s user 0.82s system 16% cpu 59.776 total

### 4 partitions
spark-submit euler.py 5000000  10.90s user 0.98s system 28% cpu 42.000 total

### 16 partitions
spark-submit euler.py 5000000  9.81s user 1.13s system 26% cpu 41.543 total

### 60 partitions
spark-submit euler.py 5000000  12.35s user 0.99s system 32% cpu 40.485 total

### 120 partitions
spark-submit euler.py 5000000  14.05s user 1.08s system 37% cpu 40.449 total

### 240 partitions
spark-submit euler.py 5000000  14.71s user 1.17s system 35% cpu 44.245 total

### Results:
It's pretty clear that there's almost no difference once I have parallelism, since the RDD is partitioned evenly. 
Once the number of partitions is too large, there is a loss of speed due to overhead.

## Running Tests to see the difference in times between different python run-time environments
1) Standard CPython Implementation
   1) Run using: 
      1) export PYSPARK_PYTHON=$PY3_PATH 
      2) time ${SPARK_HOME}/bin/spark-submit euler.py 50000000
   2) Results: 
      1) 2.7183608399999493 
      2) 10.83s user 1.13s system 3% cpu 5:28.77 total
2) Spark Python with PyPy
   1) Run using: 
      1) export PYSPARK_PYTHON=$PYPY_PATH 
      2) time ${SPARK_HOME}/bin/spark-submit euler.py 50000000
   2) Results: 
      1) 2.718353879999991 
      2) 11.39s user 1.07s system 3% cpu 5:48.50 total
3) Non-Spark Single-Threaded PyPy
    1) Run using:
       1) time $PYPY_PATH euler_single.py 1000000000
    2) Results:
       1) 2.71830131
       2) 49.84s user 0.23s system 98% cpu 51.097 total
4) Non-Spark Single-Threaded C
    1) Run using:
       1) gcc -Wall -O2 -o euler euler.c && time ./euler 1000000000
    2) Results:
       1) 2.71828
       2) 28.45s user 0.06s system 98% cpu 28.959 total
