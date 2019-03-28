#!/bin/bash

JAR_FILE="mapreduce-analysis-msd.jar"
HDFS_DATA="local"

function usage {
cat << EOF
    
    Usage: run.sh -[ 1 | 2 ] -c

    -1 : Artist Questions Q1, Q2, and Q4
    -2 : Song Questions Q3, Q5, and Q6
    
    -c : compile
    
EOF
    exit 1
}

if [ $# -lt 1 ]; then
    usage;
fi

if [ "$2" == "-c" ]; then
    find ~/.gradle -type f -name "*.lock" | while read f; do rm $f; done
    gradle build
fi

SECOND_INPUT=""

case "$1" in
    
-1) CLASS_JOB="artist"
    SECOND_INPUT="/${HDFS_DATA}/analysis/"
    ;;

    
-2) CLASS_JOB="song"
    SECOND_INPUT="/${HDFS_DATA}/analysis/"
    ;;
    
-9) CLASS_JOB="artistmost"
    ;;
    
*) usage;
    ;;
    
esac

echo ${SECOND_INPUT}

# Connect to shared HDFS
# 
# export HADOOP_CONF_DIR=${HOME}/cs455/mapreduce/client-config
# 
$HADOOP_HOME/bin/hadoop fs -rm -R /out/${CLASS_JOB} ||: \
&& $HADOOP_HOME/bin/hadoop jar build/libs/${JAR_FILE} cs455.hadoop.${CLASS_JOB}.MainJob \
/${HDFS_DATA}/metadata/ $SECOND_INPUT /out/${CLASS_JOB} \
&& $HADOOP_HOME/bin/hadoop fs -ls /out/${CLASS_JOB} \
&& $HADOOP_HOME/bin/hadoop fs -cat /out/${CLASS_JOB}/part-r-00000
