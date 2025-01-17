#!/bin/bash

hostname | grep ecehadoop
if [ $? -eq 1 ]; then
    echo "This script must be run on ecehadoop :("
    exit -1
fi

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export SCALA_HOME=/usr
export HADOOP_HOME=/opt/hadoop-latest/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop/
export SPARK_HOME=/opt/spark-latest/
#export SPARK_HOME=/opt/spark-2.4.6-bin-without-hadoop-scala-2.12/
#export SPARK_DIST_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`
MAIN_SPARK_JAR=`ls $SPARK_HOME/jars/spark-core*.jar`
export CLASSPATH=".:$MAIN_SPARK_JAR"


echo --- Deleting
rm Task1.jar
rm Task1*.class

echo --- Compiling
$SCALA_HOME/bin/scalac -J-Xmx1g Task1.scala
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf Task1.jar Task1*.class

echo --- Running
INPUT=/user/${USER}/a2_inputs/
OUTPUT=/user/${USER}/a2_starter_code_output_spark/

$HADOOP_HOME/bin/hdfs dfs -mkdir $INPUT
$HADOOP_HOME/bin/hdfs dfs -rm -R $OUTPUT
$HADOOP_HOME/bin/hdfs dfs -copyFromLocal sample_input/smalldata.txt $INPUT
time $SPARK_HOME/bin/spark-submit --master yarn --class Task1 --driver-memory 4g --executor-memory 4g Task1.jar /a2_inputs/in4.txt $OUTPUT

export HADOOP_ROOT_LOGGER="WARN"
$HADOOP_HOME/bin/hdfs dfs -ls $OUTPUT
$HADOOP_HOME/bin/hdfs dfs -cat $OUTPUT/*
