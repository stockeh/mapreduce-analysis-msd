#!/bin/bash

JAR_FILE="mapreduce-analysis-msd.jar"

# LOCAL HDFS:

HDFS_DATA="local"
OUT_DIR="/out"

function usage {
cat << EOF
    
    Usage: run.sh -[ 1 | 2 | 3 | 4] -c -s

    -1 : Song and Artist Questions Q1 - Q6, Q8
    -2 : Aggregate Analysis Q7, Q9
    -3 : Location Analysis Q10
    -4 : OLD Segment Data Q7
    
    -c : Compile
    -s : Shared HDFS
    
EOF
    exit 1
}

if [ $# -lt 1 ]; then
    usage;
fi

if [[ $* = *-c* ]]; then
    find ~/.gradle -type f -name "*.lock" | while read f; do rm $f; done
    gradle build
fi

if [[ $* = *-s* ]]; then
    export HADOOP_CONF_DIR=${HOME}/cs455/mapreduce/client-config
    HDFS_DATA="data"
    OUT_DIR="/home/out"
fi

FIRST_INPUT="/"${HDFS_DATA}"/metadata/"
SECOND_INPUT=""

case "$1" in
    
-1) CLASS_JOB="basic"
    SECOND_INPUT="/${HDFS_DATA}/analysis/"
    ;;
    
-2) CLASS_JOB="aggregate"
    SECOND_INPUT="/${HDFS_DATA}/analysis/"
    ;;   

-3) CLASS_JOB="location"
    ;;   
  
-4) CLASS_JOB="segment"
    FIRST_INPUT="/${HDFS_DATA}/analysis/"
    ;;   
     
*) usage;
    ;;
    
esac

LINES=`find . -name "*.java" -print | xargs wc -l | grep "total" | awk '{$1=$1};1'`

echo Project has "$LINES" lines

$HADOOP_HOME/bin/hadoop fs -rm -R ${OUT_DIR}/${CLASS_JOB} ||: \
&& $HADOOP_HOME/bin/hadoop jar build/libs/${JAR_FILE} cs455.hadoop.${CLASS_JOB}.MainJob \
$FIRST_INPUT $SECOND_INPUT ${OUT_DIR}/${CLASS_JOB} \
&& $HADOOP_HOME/bin/hadoop fs -ls ${OUT_DIR}/${CLASS_JOB} \
&& $HADOOP_HOME/bin/hadoop fs -cat ${OUT_DIR}/${CLASS_JOB}/*
