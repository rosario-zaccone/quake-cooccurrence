#!/bin/bash
set -e
source ./scripts/config.sh


echo "Setting active project..."
gcloud config set project $PROJECT_ID
gcloud config set compute/region $REGION
gcloud config set compute/zone $ZONE

echo "Enabling required APIs..."
gcloud services enable dataproc.googleapis.com
gcloud services enable compute.googleapis.com
gcloud services enable storage.googleapis.com

echo "Checking authentication..."
gcloud auth list

echo "GCP environment setup completed successfully."
