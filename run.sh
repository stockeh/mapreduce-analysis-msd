#!/bin/bash

JAR_FILE="mapreduce-analysis-msd.jar"
HDFS_DATA="local"

function usage {
cat << EOF
    
    Usage: run.sh -[ 1 | 2 ] -c

    -1 : Artist With Most Songs
    -2 : Artist's With Loudest Songs
    
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

-1) CLASS_JOB="artistmost"
    ;;
    
-2) CLASS_JOB="artistloud"
    SECOND_INPUT="/${HDFS_DATA}/analysis/"
    ;;

*) usage;
    ;;
esac

echo ${SECOND_INPUT}

# Connect to shared HDFS
# 
# export HADOOP_CONF_DIR=${HOME}/cs455/mapreduce/client-config
# 
$HADOOP_HOME/bin/hadoop fs -rm -R /analysis/out/${CLASS_JOB} ||: \
&& $HADOOP_HOME/bin/hadoop jar build/libs/${JAR_FILE} cs455.hadoop.${CLASS_JOB}.MainJob \
/${HDFS_DATA}/metadata/ $SECOND_INPUT /analysis/out/${CLASS_JOB} \
&& $HADOOP_HOME/bin/hadoop fs -ls /analysis/out/${CLASS_JOB} \
&& $HADOOP_HOME/bin/hadoop fs -head /analysis/out/${CLASS_JOB}/part-r-00000
