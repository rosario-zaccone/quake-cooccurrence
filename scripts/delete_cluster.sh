#!/bin/bash
set -e
source ./scripts/config.sh


if ! gcloud dataproc clusters describe $CLUSTER_NAME --region=$REGION >/dev/null 2>&1; then
    echo "Cluster $CLUSTER_NAME does not exist."
    exit 0
fi

echo "Deleting cluster $CLUSTER_NAME..."

gcloud dataproc clusters delete $CLUSTER_NAME \
    --region=$REGION \
    --quiet

echo "Cluster deleted successfully."
