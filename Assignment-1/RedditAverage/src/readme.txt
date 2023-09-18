d#### JSON Input & Reddit Comments ####

To compile with the JSON JAR file and LongPairWritable

	export HADOOP_CLASSPATH=./json-20180813.jar
	${JAVA_HOME}/bin/javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` LongPairWritable.java RedditAverage.java
	${JAVA_HOME}/bin/jar cf a1.jar *.class

To run locally

	with default reducers and no combiner:
	${HADOOP_HOME}/bin/yarn jar a1.jar RedditAverage reddit-1 output-1  7.44s user 0.77s system 195% cpu 4.193 total
	${HADOOP_HOME}/bin/yarn jar a1.jar RedditAverage reddit-2 output-2  16.33s user 1.19s system 162% cpu 10.811 total

	with 0 reducers:
	${HADOOP_HOME}/bin/yarn jar a1.jar RedditAverage -D mapreduce.job.reduces=0 reddit-1 output-1-noreduce
	${HADOOP_HOME}/bin/yarn jar a1.jar RedditAverage -D mapreduce.job.reduces=0 reddit-2 output-2-noreduce

	with the combiner:
	${HADOOP_HOME}/bin/yarn jar a1.jar RedditAverage reddit-1 output-1-combiner  7.29s user 0.78s system 210% cpu 3.826 total
	${HADOOP_HOME}/bin/yarn jar a1.jar RedditAverage reddit-2 output-2-combiner  16.00s user 0.93s system 169% cpu 9.994 total
