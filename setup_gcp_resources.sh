# Enable required Google Cloud APIs
gcloud services enable pubsub.googleapis.com dataflow.googleapis.com bigquery.googleapis.com storage.googleapis.com

# Create Pub/Sub topics
gcloud pubsub topics create orders-topic
gcloud pubsub topics create inventory-topic

# Create Pub/Sub subscriptions
gcloud pubsub subscriptions create orders-sub --topic=orders-topic
gcloud pubsub subscriptions create inventory-sub --topic=inventory-topic

# Get the current GCP project ID
PROJECT_ID=$(gcloud config get-value project)

# Create a Cloud Storage bucket for Dataflow temporary files
gsutil mb -l us-central1 gs://$PROJECT_ID-bucket

# Create BigQuery dataset
bq --location=US mk --dataset $PROJECT_ID:ecommerce

echo "GCP setup complete for project: $PROJECT_ID"
