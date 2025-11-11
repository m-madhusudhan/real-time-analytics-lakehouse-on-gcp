# Real-Time E-commerce Data Pipeline using Google Cloud Platform

This project demonstrates a complete real-time and batch data processing pipeline on Google Cloud Platform. It simulates e-commerce order and inventory data, streams it using Pub/Sub, processes it through Dataflow (Apache Beam), and stores the results in BigQuery for analytics.

---

## Project Overview

The goal of this project is to build an end-to-end data engineering solution that handles both streaming and batch data. The pipeline ingests simulated orders and inventory updates in real time, processes them using Apache Beam, and makes the results available for analysis in BigQuery.

---

## Architecture

The data pipeline uses GCP services to process and store streaming and batch data efficiently.

**Data Flow:**  
Producers (Orders, Inventory)  
↓  
Pub/Sub Topics  
↓  
Dataflow Pipelines  
↓  
BigQuery Tables  
↓  
Curated Joined Table  
↓  
Analytics and Visualization

**Components:**

1. **Pub/Sub** for streaming data ingestion  
2. **Dataflow (Apache Beam)** for data transformation and loading  
3. **BigQuery** for storage and analytics  
4. **Cloud Storage (GCS)** for batch files and temporary pipeline storage  
5. **Python Producers** to simulate real-time data feeds  

---

## Step 1: Enable APIs and Create Resources

Save these commands in a script called `setup_gcp_resources.sh`:

```bash
gcloud services enable pubsub.googleapis.com dataflow.googleapis.com bigquery.googleapis.com storage.googleapis.com

gcloud pubsub topics create orders-topic
gcloud pubsub topics create inventory-topic

gcloud pubsub subscriptions create orders-sub --topic=orders-topic
gcloud pubsub subscriptions create inventory-sub --topic=inventory-topic

PROJECT_ID=$(gcloud config get-value project)
gsutil mb -l us-central1 gs://$PROJECT_ID-bucket

bq --location=US mk --dataset $PROJECT_ID:ecommerce
