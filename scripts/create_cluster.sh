#!/bin/bash
set -e
source ./scripts/config.sh


if gcloud dataproc clusters describe $CLUSTER_NAME --region=$REGION >/dev/null 2>&1; then
    echo "Cluster $CLUSTER_NAME already exists."
    exit 0
fi

echo "Creating Dataproc cluster..."

gcloud dataproc clusters create $CLUSTER_NAME \
    --region=$REGION \
    --zone=$ZONE \
    --num-workers=$NUM_WORKERS \
    --master-boot-disk-size 240 \
    --worker-boot-disk-size 240 \
    --worker-machine-type=$MACHINE_TYPE \
    --master-machine-type=$MACHINE_TYPE \
    --image-version=$IMAGE_VERSION \
    --bucket=$TEMP_BUCKET \
    --network=$NETWORK \
    --enable-component-gateway \
    --optional-components=JUPYTER \
    --max-idle=30m

echo "Cluster created successfully."
