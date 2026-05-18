#!/bin/bash

# =====================================
# GCP CONFIGURATION
# =====================================

export PROJECT_ID="project-0e93de98-3aae-4b4f-a2a"
export REGION="europe-west1"

# =====================================
# DATAPROC CONFIGURATION
# =====================================

export NUM_WORKERS=4
export CLUSTER_NAME="spark-cluster-4w"
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

export LOCAL_FILE="./data/dataset.csv"
export DEST_FILE_NAME="dataset.csv"