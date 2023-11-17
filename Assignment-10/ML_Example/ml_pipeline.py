import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('ML Pipeline').getOrCreate()
assert spark.version >= '3.0'
spark.sparkContext.setLogLevel('WARN')


from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator


def main():
    # Create some data
    data = spark.range(100000)  # Creates an empty dataframe with an id col from (0 - 99,999)
    data = data.select(
        (functions.rand()*100).alias('length'),
        (functions.rand()*100).alias('width'),
        (functions.rand()*100).alias('height'),
    )
    data = data.withColumn('volume', data['length']*data['width']*data['height'])

    # Split the data into training and validation
    training, validation = data.randomSplit([0.75, 0.25])

    # input Cols are assembled into a feature vector
    assemble_features = VectorAssembler(
        inputCols=['length', 'width', 'height'],
        outputCol='features')

    # featuresCol is used to predict the labelCol
    regressor = GBTRegressor(
        featuresCol='features', labelCol='volume')
    pipeline = Pipeline(stages=[assemble_features, regressor])

    # Fit the model
    model = pipeline.fit(training)

    # See how we performed on the validation set
    predictions = model.transform(validation)
    predictions.show()
    
    r2_evaluator = RegressionEvaluator(
        predictionCol='prediction', labelCol='volume',
        metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)
    print(r2)
    

if __name__ == '__main__':
    main()
