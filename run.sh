#!/bin/bash

JAR_FILE="mapreduce-analysis-msd.jar"
HDFS_DATA="local"

function usage {
cat << EOF
    
    Usage: run.sh -[ 1 | 2 ] -c

    -1 : Song and Artist Questions Q1 - Q6
    -2 : 
    
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
    
-1) CLASS_JOB="basic"
    SECOND_INPUT="/${HDFS_DATA}/analysis/"
    ;;
   
*) usage;
    ;;
    
esac

LINES=`find . -name "*.java" -print | xargs wc -l | grep "total" | awk '{$1=$1};1'`

echo Project has "$LINES" lines

# Connect to shared HDFS
# 
# export HADOOP_CONF_DIR=${HOME}/cs455/mapreduce/client-config
# 
$HADOOP_HOME/bin/hadoop fs -rm -R /out/${CLASS_JOB} ||: \
&& $HADOOP_HOME/bin/hadoop jar build/libs/${JAR_FILE} cs455.hadoop.${CLASS_JOB}.MainJob \
/${HDFS_DATA}/metadata/ $SECOND_INPUT /out/${CLASS_JOB} \
&& $HADOOP_HOME/bin/hadoop fs -ls /out/${CLASS_JOB} \
&& $HADOOP_HOME/bin/hadoop fs -cat /out/${CLASS_JOB}/part-r-00000
