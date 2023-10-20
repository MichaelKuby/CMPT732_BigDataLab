####WordCount####

Compile Java Code Locally:

Compile Classes:
	${JAVA_HOME}/bin/javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` WordCount.java
	Where WordCount.java is the name of the file to be compiled.

Compile .jar file
	${JAVA_HOME}/bin/jar cf wordcount.jar WordCount*.class

	or for the latter part of the exercise use

	${JAVA_HOME}/bin/jar cf wordcount.jar WordCountImproved*.class

	WordCount*.class are the class files to cull from. We ended up using WordCountImproved*.class
	wordcount.jar is the name of the jar file to be created.

Run Locally:

	Run:
	${HADOOP_HOME}/bin/yarn jar wordcount.jar WordCount wordcount-1 output-1
	${HADOOP_HOME}/bin/yarn jar wordcount.jar WordCountImproved wordcount-1 wordsep-output-1
	
	Read output:
	less output-1/part-*
	less wordsep-output-1/part-*

Code explanation:

Certainly! This is a Java program that uses the Hadoop MapReduce framework to perform a word count operation on a large dataset. Given your focus on Big Data in your Master of Science in Professional Computing Science, you'll find this example particularly relevant. Let's break down the code:

### Import Statements
The code starts by importing various classes from the Hadoop library and Java's standard library.

### Class Definition
The `WordCount` class extends `Configured` and implements the `Tool` interface. This allows the class to be configured and run as a Hadoop job.

### Mapper Class: `TokenizerMapper`
This class extends `Mapper` and is responsible for the "Map" part of the MapReduce job. It takes a line of text, tokenizes it into words, and emits each word with a count of 1.

- `LongWritable, Text`: Input key-value pair types. Here, `LongWritable` is the line offset in the file, and `Text` is the line itself.
- `Text, IntWritable`: Output key-value pair types. Here, `Text` is the word, and `IntWritable` is the count (always set to 1).

### Reducer Class: `IntSumReducer`
This class extends `Reducer` and is responsible for the "Reduce" part of the MapReduce job. It takes all the values (counts) for each unique word and sums them up.

- `Text, IntWritable`: Input key-value pair types. These are the same as the output types from the Mapper.
- `Text, IntWritable`: Output key-value pair types. The output is the unique word and its total count.

### Main Method
The `main` method initializes the Hadoop job and sets various configurations.

### `run` Method
This method sets up the job's configurations, such as input/output formats, Mapper and Reducer classes, and input/output paths. It then waits for the job to complete.

Here's a quick summary of the flow:

1. The `main` method calls `ToolRunner.run`, which in turn calls the `run` method.
2. The `run` method sets up the job and waits for it to complete.
3. The `TokenizerMapper` class tokenizes each line of input and emits a word with a count of 1.
4. The `IntSumReducer` class sums up the counts for each unique word.

I hope this explanation helps you understand the code better! If you have any more questions, feel free to ask.