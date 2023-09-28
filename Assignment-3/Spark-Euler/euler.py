from pyspark import SparkConf, SparkContext
import sys
import random

# Use random.uniform(a, b)
# Return a random floating point number N such that a <= N <= b for a <= b and b <= N <= a for b < a.
#
# The end-point value b may or may not be included in the range depending on floating-point rounding in the equation a + (b-a) * random().

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


# add more functions as necessary

# Sample random numbers between 0 and 1 until their sum is greater than 1.
def sum_greater_one(n):
    random.seed()
    sum_double = 0.0
    nums = []
    while sum_double < 1.0:
        new_num = random.uniform(0,1)
        nums.append(new_num)
        sum_double += new_num
    return len(nums)


def add_pair(tuple1, tuple2):
    one1, num1 = tuple1
    one2, num2 = tuple2
    return one1 + one2, num1 + num2


def main(inputs):
    # Choose the number of partitions
    numSlices = 16

    # inputs is an int val from the command-line
    samples = int(inputs)

    # Create a range the size of the number of samples
    indices = range(samples)

    # Create an RDD of the indices for each record. Set numSlices to something reasonable.
    indices_RDD = sc.parallelize(indices, numSlices=numSlices)

    # Map sum_greater_one() over each index to create the sample points V = min {n | x_1 + x_2 + ... + x_n > 1}
    sums_greater_than_ones_lens = indices_RDD.map(sum_greater_one)
    #sums_greater_than_ones_lens.saveAsTextFile('output')

    # Compute the average
    eulers_num = sums_greater_than_ones_lens.mean()

    # Output is to simply print the result at the end of main
    print(eulers_num)


# main logic starts here

if __name__ == '__main__':
    # Spark RDD Set-up
    conf = SparkConf().setAppName('euler.py')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    main(inputs)
