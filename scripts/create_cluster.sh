#!/bin/bash
set -e
source ./scripts/config.sh

NUM_WORKERS=${1:-$NUM_WORKERS}
CLUSTER_NAME="spark-cluster-${NUM_WORKERS}w"

if gcloud dataproc clusters describe "$CLUSTER_NAME" --region="$REGION" >/dev/null 2>&1; then
    echo "Cluster $CLUSTER_NAME already exists."
    exit 0
fi

echo "Creating Dataproc cluster $CLUSTER_NAME..."

echo "Using standard configuration with $NUM_WORKERS worker(s)..."

gcloud dataproc clusters create "$CLUSTER_NAME" \
    --region="$REGION" \
    --num-workers="$NUM_WORKERS" \
    --master-boot-disk-size 240 \
    --worker-boot-disk-size 240 \
    --worker-machine-type="$MACHINE_TYPE" \
    --master-machine-type="$MACHINE_TYPE" \
    --bucket="$TEMP_BUCKET" \
    --network="$NETWORK" \
    --enable-component-gateway \
    --max-idle=600m

echo "Cluster $CLUSTER_NAME created successfully."