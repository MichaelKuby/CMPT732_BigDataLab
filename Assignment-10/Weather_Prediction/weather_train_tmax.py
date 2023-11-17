import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler, SQLTransformer

tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])


def main(data_directory, output_directory):
    data = spark.read.csv(data_directory, schema=tmax_schema)
    train, validation = data.randomSplit([0.75, 0.25])

    # Do we need to cache? Maybe not. Do it and decide later.
    train.cache()
    validation.cache()

    # TODO: transform date -> day of year.
    query = "SELECT station, dayofyear(date) AS DOTY, latitude, longitude, elevation, tmax FROM __THIS__"
    sqlTrans = SQLTransformer(statement=query)

    # TODO: create assembler
    assembler = VectorAssembler(inputCols=['DOTY', 'latitude', 'longitude', 'elevation'], outputCol='features')

    # TODO: decide on a classifier
    random_forest_regressor = RandomForestRegressor(featuresCol='features', labelCol='tmax',
                                                    predictionCol='prediction', maxDepth=10, maxBins=80,
                                                    minInstancesPerNode=1, minInfoGain=0.0, numTrees=80)

    # TODO: create the pipeline
    pipeline = Pipeline(stages=[sqlTrans, assembler, random_forest_regressor])

    # TODO: Train the model
    rf_model = pipeline.fit(train)

    # TODO: Make some predictions
    predictions = rf_model.transform(validation)

    # TODO: create an evaluator and score the validation data
    regression_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax', metricName='r2')
    r2 = regression_evaluator.evaluate(predictions)
    print(predictions.show())
    print(r2)

    rf_model.write().overwrite().save(output_directory)

if __name__ == '__main__':
    # Spark DataFrame set-up
    data_directory = sys.argv[1]
    output_directory = sys.argv[2]
    spark = SparkSession.builder.appName('tmax train').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(data_directory, output_directory)
