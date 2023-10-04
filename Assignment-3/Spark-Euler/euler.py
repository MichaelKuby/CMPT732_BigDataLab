from pyspark import SparkConf, SparkContext
import sys
import random

# Use random.uniform(a, b)
# Return a random floating point number N such that a <= N <= b for a <= b and b <= N <= a for b < a.
#
# The end-point value b may or may not be included in the range depending on
# floating-point rounding in the equation a + (b-a) * random().

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


# Sample random numbers between 0 and 1 until their sum is greater than 1, n number of times, and compute the average
# number of values it requires.
def sum_greater_one(batch_size):
    random.seed()
    count = 0
    for i in range(batch_size):
        sum = 0.0
        while sum < 1.0:
            new_num = random.uniform(0,1)
            sum += new_num
            count += 1
    return count / batch_size


def main(inputs):

    samples = int(inputs)

    # Choose the number of batches
    num_batches = 100

    # Compute the size of each batch
    batch_size = samples // num_batches

    # Choose the number of partitions per RDD
    num_slices = 40

    # Create an RDD of the indices for each record. Set numSlices to something reasonable.
    indices_RDD = sc.range(num_batches, numSlices=num_slices)

    # Map sum_greater_one(batch_size) over each index to create the samples V = min {n | x_1 + x_2 + ... + x_n > 1}
    counts = indices_RDD.map(lambda x: sum_greater_one(batch_size))

    # Output is to simply print the result at the end of main
    print(counts.mean())


if __name__ == '__main__':
    # Spark RDD Set-up
    conf = SparkConf().setAppName('euler.py')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    main(inputs)
