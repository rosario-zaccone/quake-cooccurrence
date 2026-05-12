#!/bin/bash
set -e
source ./scripts/config.sh


delete_bucket_if_exists () {
    BUCKET_NAME=$1

    if gsutil ls -b gs://$BUCKET_NAME >/dev/null 2>&1; then
        echo "Deleting bucket gs://$BUCKET_NAME..."
        gsutil -m rm -r gs://$BUCKET_NAME
    else
        echo "Bucket gs://$BUCKET_NAME does not exist."
    fi
}

delete_bucket_if_exists $DATA_BUCKET
delete_bucket_if_exists $TEMP_BUCKET

echo "Buckets deleted successfully."
