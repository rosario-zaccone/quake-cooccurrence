#!/bin/bash
set -e
source ./scripts/config.sh 

upload_file_to_gcs() {
    local LOCAL_FILE=$1
    local DEST_BUCKET=$2
    local DEST_FILE_NAME=$3

    if [ ! -f "$LOCAL_FILE" ]; then
        echo "File $LOCAL_FILE not found!"
        exit 1
    fi

    echo "Uploading $LOCAL_FILE to gs://$DEST_BUCKET/$DEST_FILE_NAME ..."
    gsutil cp "$LOCAL_FILE" "gs://$DEST_BUCKET/$DEST_FILE_NAME"

    echo "File uploaded to gs://$DEST_BUCKET/$DEST_FILE_NAME"
}


if [ -z "$LOCAL_FILE" ] || [ -z "$DATA_BUCKET" ] || [ -z "$DEST_FILE_NAME" ]; then
    echo "Variables LOCAL_FILE, DATA_BUCKET or DEST_FILE_NAME not defined!"
    exit 1
fi


upload_file_to_gcs "$LOCAL_FILE" "$DATA_BUCKET" "$DEST_FILE_NAME"
