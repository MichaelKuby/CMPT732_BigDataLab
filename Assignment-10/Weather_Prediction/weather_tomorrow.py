import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from datetime import date

spark = SparkSession.builder.appName('tmax model tester').getOrCreate()
assert spark.version >= '2.3'  # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import PipelineModel

tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])


def test_model(model_file):
    # create the data
    due_date = [("SFU_Campus", date(2023, 11, 17), 49.2771, -122.9146, 330.0, 12.0),
                ("SFU_Campus", date(2023, 11, 18), 49.2771, -122.9146, 330.0, 0.0)]
    due_dateDF = spark.createDataFrame(data=due_date, schema=tmax_schema)

    # load the model
    model = PipelineModel.load(model_file)

    # use the model to make predictions
    predictions = model.transform(due_dateDF)
    prediction = predictions.first()    # Okay to collect -> a single value

    print('Predicted tmax tomorrow:', prediction['prediction'])

    # The value I'm getting is 8.297, and I interestingly found here:
    # https://www.wunderground.com/history/daily/ca/richmond/CYVR/date/2022-11-18
    # that the historical average for November 18 in Richmond (not Burnaby, but the best I could find) is
    # 47.6 F = 8.67 C


if __name__ == '__main__':
    model_file = sys.argv[1]
    test_model(model_file)
