import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# Project configuration
PROJECT_ID = "qwiklabs-gcp-01-cffc452c1796"
REGION = "us-central1"
PUBSUB_TOPIC = f"projects/{PROJECT_ID}/topics/inventory-topic"
TEMP_LOCATION = f"gs://{PROJECT_ID}-bucket/temp"
BIGQUERY_TABLE = f"{PROJECT_ID}:ecommerce.inventory_stream"

# Beam DoFn to parse inventory messages
class ParseInventory(beam.DoFn):
    def process(self, element):
        rec = json.loads(element)
        rec["quantity_in_stock"] = int(rec["quantity_in_stock"])
        rec["last_updated"] = float(rec["last_updated"])
        return [rec]

def run():
    # Pipeline options
    options = PipelineOptions(
        streaming=True,
        project=PROJECT_ID,
        region=REGION,
        temp_location=TEMP_LOCATION,
        job_name="inventory-stream-job"
    )
    options.view_as(StandardOptions).streaming = True

    # Define the pipeline
    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadPubSub" >> beam.io.ReadFromPubSub(topic=PUBSUB_TOPIC)
            | "Decode" >> beam.Map(lambda x: x.decode("utf-8"))
            | "ParseInventory" >> beam.ParDo(ParseInventory())
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                table=BIGQUERY_TABLE,
                schema=("inventory_id:STRING,product_id:INTEGER,warehouse_id:STRING,"
                        "quantity_in_stock:INTEGER,last_updated:FLOAT"),
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == "__main__":
    run()