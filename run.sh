#!/bin/bash

JAR_FILE="mapreduce-analysis-msd.jar"

## Compile project

find ~/.gradle -type f -name "*.lock" | while read f; do rm $f; done
gradle build

## Connect to shared HDFS

# export HADOOP_CONF_DIR=${HOME}/cs455/mapreduce/client-config

$HADOOP_HOME/bin/hadoop fs -rm -R /analysis/out ||: \
&& $HADOOP_HOME/bin/hadoop jar build/libs/${JAR_FILE} cs455.hadoop.analysis.WordCountJob /local/metadata/metadata1.csv /analysis/out \
&& $HADOOP_HOME/bin/hadoop fs -ls /analysis/out
