#!/bin/bash
# Simple Spark Scala job submit

set -e
source ./scripts/config.sh


if [ $# -lt 2 ]; then
    echo "Usage: ./submit_job.sh myjob.jar com.example.Main [args...]"
    exit 1
fi

JAR_FILE=$1
MAIN_CLASS=$2
shift 2

# Upload JAR to DATA_BUCKET
JOB_NAME=$(basename $JAR_FILE)
GCS_JAR_PATH="gs://$DATA_BUCKET/jobs/$JOB_NAME"
echo "Uploading $JAR_FILE to $GCS_JAR_PATH..."
gsutil cp $JAR_FILE $GCS_JAR_PATH

# Submit job to Dataproc
echo "Submitting Spark job..."
gcloud dataproc jobs submit spark \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --class=$MAIN_CLASS \
    --jars $GCS_JAR_PATH \
    -- "$@"


echo "Job submitted!"
