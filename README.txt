Run "mvn clean package" in the directory containing the pom.xml file to generate
the hadoop-exercise-0.0.1-SNAPSHOT.jar file

Run "mvn test" in the directory containing the pom.xml file to execute the unit
tests for the UFORecordReader parse and toString methods

To put the ufo_awesome.tsv input file in HDFS and create output directories run
the following commands as the cloudera user:
hdfs dfs -mkdir -p /user/cloudera/input
hdfs dfs -copyFromLocal <path to project directory>/hadoop-exercise/src/main/resources/ufo_awesome.tsv /user/cloudera/input/ufo_awesome.tsv
hdfs dfs -mkdir -p /user/cloudera/shapecount/intermediate
hdfs dfs -mkdir -p /user/cloudera/shapecount/final
hdfs dfs -mkdir -p /user/cloudera/yearstatecount

Input directory for both jobs:  /user/cloudera/input

To run the shape count job as the cloudera user:
hadoop jar <path to project directory>/hadoop-exercise/target/hadoop-exercise-0.0.1-SNAPSHOT.jar hadoopExercise.ShapeCount

Output directory for shape count:  /user/cloudera/shapecount/output

To view the results of shape count job run the following as the cloudera user:
hdfs dfs -cat /user/cloudera/shapecount/final/output/part-00000

To run the year state count job as the cloudera user:
hadoop jar <path to project directory>/hadoop-exercise/target/hadoop-exercise-0.0.1-SNAPSHOT.jar hadoopExercise.YearStateCount

Output directory for year state count:
/user/cloudera/yearstatecount/final/output

To view the results of year state count job run the following command as the
cloudera user:
hdfs dfs -cat /user/cloudera/yearstatecount/output/part-00000

Note:
To simplify the Hadoop commands for running the jobs above the HDFS input and
output directories have been defined as constants in their respective files
which are the only platform dependent code in the project.