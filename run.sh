#!/bin/bash

JAR_FILE="mapreduce-analysis-msd.jar"

# gradle clean; gralde build;

export HADOOP_CONF_DIR=${HOME}/cs455/mapreduce/client-config

$HADOOP_HOME/bin/hadoop fs -rm -R /home/analysis/out ||: \
&& $HADOOP_HOME/bin/hadoop jar build/libs/${JAR_FILE} cs455.hadoop.analysis.WordCountJob /data/metadata/metadata1.csv /home/analysis/out \
&& $HADOOP_HOME/bin/hadoop fs -ls /home/analysis/out
