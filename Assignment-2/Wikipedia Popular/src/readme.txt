Objective: Create a MapReduce class WikipediaPopular that finds the number of times the most-visited page was 
visited each hour.

	Number of times: A count

	Most visited page: There will be a single result

	Each hour: Because the input files are structured by the hour, each input file will have a single result.

File Structure:

	Date-Hour language page_name requests bytes_returned

Compile to generate classes:
	${JAVA_HOME}/bin/javac -classpath `${HADOOP_HOME}/bin/hadoop classpath` WikipediaPopular.java
	Where WikipediaPopular.java is the name of the file to be compiled.

Compile .jar file
        ${JAVA_HOME}/bin/jar cf WikipediaPopular.jar WikipediaPopular*.class

        *.class are the class files to cull from.
        .jar is the name of the jar file to be created.

Run Locally:

	In general:
	${HADOOP_HOME}/bin/yarn jar jarfiletorun.jar class_to_run inputfile outputfile
	
	In particular:
	${HADOOP_HOME}/bin/yarn jar WikipediaPopular.jar WikipediaPopular pagecounts-with-time-0 output-0

	Read Local Output:
	less output-0/part-*
