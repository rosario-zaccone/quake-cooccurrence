#!/bin/bash
set -e
source ./scripts/config.sh


create_bucket_if_not_exists () {
    BUCKET_NAME=$1

    if gsutil ls -b gs://$BUCKET_NAME >/dev/null 2>&1; then
        echo "Bucket gs://$BUCKET_NAME already exists."
    else
        echo "Creating bucket gs://$BUCKET_NAME..."
        gsutil mb -p $PROJECT_ID -l $BUCKET_LOCATION gs://$BUCKET_NAME
        gsutil uniformbucketlevelaccess set on gs://$BUCKET_NAME
    fi
}

echo "Creating DATA bucket..."
create_bucket_if_not_exists $DATA_BUCKET

echo "Creating TEMP bucket..."
create_bucket_if_not_exists $TEMP_BUCKET

echo "Applying lifecycle policy to TEMP bucket (auto-delete after 7 days)..."

cat > lifecycle.json <<EOF
{
  "rule": [
    {
      "action": {"type": "Delete"},
      "condition": {"age": 7}
    }
  ]
}
EOF

gsutil lifecycle set lifecycle.json gs://$TEMP_BUCKET
rm lifecycle.json

echo "Creating logical folders in DATA bucket..."
gsutil cp /dev/null gs://$DATA_BUCKET/data/.keep || true
gsutil cp /dev/null gs://$DATA_BUCKET/output/.keep || true
gsutil cp /dev/null gs://$DATA_BUCKET/jobs/.keep || true
gsutil cp /dev/null gs://$DATA_BUCKET/logs/.keep || true

echo "Buckets successfully configured."
