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