#!/bin/bash

# =====================================
# GCP CONFIGURATION
# =====================================

export PROJECT_ID="scalableunibo2026"
export REGION="europe-west1"
export ZONE="europe-west1-b"

# =====================================
# DATAPROC CONFIGURATION
# =====================================

export NUM_WORKERS=2 # test with 2, 3 and 4
export CLUSTER_NAME="spark-cluster-${NUM_WORKERS}w"
export MACHINE_TYPE="n2-standard-4"
export IMAGE_VERSION="2.1-debian11"
export NETWORK="default"

# =====================================
# STORAGE CONFIGURATION
# =====================================

export DATA_BUCKET="${PROJECT_ID}-spark-data"
export TEMP_BUCKET="${PROJECT_ID}-spark-temp"
export BUCKET_LOCATION="${REGION}"
