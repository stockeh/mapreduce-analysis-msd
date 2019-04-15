#!/bin/bash

JAR_FILE="runner_2.11-1.0.jar"
OUT_DIR="/out"
CORE_HDFS="hdfs://providence:30210"

function usage {
cat << EOF
    
    Usage: spark-run.sh -[ 1 ] -c -s

    -1 : SBT Spark Runner
    
    -c : Compile
    
EOF
    exit 1
}

function spark_runner {
    $HADOOP_HOME/bin/hadoop fs -rm -R ${OUT_DIR}/${CLASS_JOB} ||: \
    && $SPARK_HOME/bin/spark-submit --master spark://salem.cs.colostate.edu:30136 \
    --deploy-mode cluster --class Runner target/scala-2.11/${JAR_FILE} ${INPUT} ${OUTPUT}
}

if [ $# -lt 1 ]; then
    usage;
fi

if [[ $* = *-c* ]]; then
    sbt package
    LINES=`find . -name "*.scala" -print | xargs wc -l | grep "total" | awk '{$1=$1};1'`
    echo Project has "$LINES" lines
fi

case "$1" in
    
-1) CLASS_JOB="spark"
    INPUT="${CORE_HDFS}/local/tmp/sample_multiclass_classification_data.txt"
    OUTPUT="${CORE_HDFS}${OUT_DIR}/${CLASS_JOB}"
    spark_runner
    ;;
    
*) usage;
    ;;
    
esac
