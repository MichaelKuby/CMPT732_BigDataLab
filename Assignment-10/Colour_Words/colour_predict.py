import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4'  # make sure we have Spark 2.4+

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from colour_tools import colour_schema, rgb2lab_query, plot_predictions


def main(inputs):
    data = spark.read.csv(inputs, schema=colour_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    # TODO: create a pipeline to predict RGB colours -> word
    rgb_assembler = VectorAssembler(inputCols=['R', 'G', 'B'], outputCol='features')
    rgb_word_indexer = StringIndexer(inputCol='word', outputCol='label')

    # Decide what kind of classifier to use. I am using a model that expects 3 input features, has two hidden
    # layers of 30 features each, and can classify 11 possible outputs.
    classifier = MultilayerPerceptronClassifier(layers=[3, 300, 11])

    rgb_pipeline = Pipeline(stages=[rgb_assembler, rgb_word_indexer, classifier])
    rgb_model = rgb_pipeline.fit(train)

    # Make some predictions
    rgb_predictions = rgb_model.transform(validation)

    # TODO: create an evaluator and score the validation data
    rgb_evaluator = MulticlassClassificationEvaluator(predictionCol='prediction', labelCol='label',
                                                     metricName='accuracy')
    rgb_score = rgb_evaluator.evaluate(rgb_predictions)

    plot_predictions(rgb_model, 'RGB', labelCol='word')
    print('Validation score for RGB model: %g' % (rgb_score, ))

    rgb_to_lab_query = rgb2lab_query(passthrough_columns=['word'])

    # TODO: create a pipeline RGB colours -> LAB colours -> word; train and evaluate.

    # Assemble the pieces of the pipeline
    sqlTrans = SQLTransformer(statement=rgb_to_lab_query)
    lab_assembler = VectorAssembler(inputCols=['labL', 'labA', 'labB'], outputCol='features')
    lab_word_indexer = StringIndexer(inputCol='word', outputCol='label')

    # Assemble the pipeline itself
    lab_pipeline = Pipeline(stages=[sqlTrans, lab_assembler, lab_word_indexer, classifier])
    lab_model = lab_pipeline.fit(train)

    # Make some predictions with the lab model
    lab_predictions = lab_model.transform(validation)

    # Evaluate the predictions
    lab_evaluator = MulticlassClassificationEvaluator(predictionCol='prediction', labelCol='label',
                                                      metricName='accuracy')
    lab_score = lab_evaluator.evaluate(lab_predictions)

    # Plot the predictions and assess the model
    plot_predictions(lab_model, 'LAB', labelCol='word')
    print('Validation score for LAB model:', lab_score)


if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)
