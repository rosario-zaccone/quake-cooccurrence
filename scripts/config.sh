#!/bin/bash

# =====================================
# GCP CONFIGURATION
# =====================================

export PROJECT_ID="scalableunibo2026"
export REGION="europe-west1"

# =====================================
# DATAPROC CONFIGURATION
# =====================================

export NUM_WORKERS=2
export CLUSTER_NAME="spark-cluster-2w"
export MACHINE_TYPE="n2-standard-4"
export NETWORK="default"

# =====================================
# STORAGE CONFIGURATION
# =====================================

export DATA_BUCKET="${PROJECT_ID}-spark-data"
export TEMP_BUCKET="${PROJECT_ID}-spark-temp"
export BUCKET_LOCATION="${REGION}"

# =====================================
# FILE TO UPLOAD
# =====================================

export LOCAL_FILE="./data/trimmed.csv"
export DEST_FILE_NAME="dataset_trimmed.csv"   