import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# Replace with your actual project ID
PROJECT_ID = "qwiklabs-gcp-01-cffc452c1796"
REGION = "us-central1"
PUBSUB_TOPIC = f"projects/{PROJECT_ID}/topics/orders-topic"
TEMP_LOCATION = f"gs://{PROJECT_ID}-bucket/temp"
BIGQUERY_TABLE = f"{PROJECT_ID}:ecommerce.orders_stream"

# Beam DoFn to parse orders
class ParseOrders(beam.DoFn):
    def process(self, element):
        rec = json.loads(element)
        rec["price"] = float(rec["price"])
        rec["quantity"] = int(rec["quantity"])
        rec["timestamp"] = float(rec["timestamp"])
        return [rec]

def run():
    # Set up pipeline options
    options = PipelineOptions(
        streaming=True,
        project=PROJECT_ID,
        region=REGION,
        temp_location=TEMP_LOCATION,
        job_name="orders-stream-job"
    )
    options.view_as(StandardOptions).streaming = True

    # Define the Beam pipeline
    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadPubSub" >> beam.io.ReadFromPubSub(topic=PUBSUB_TOPIC)
            | "Decode" >> beam.Map(lambda x: x.decode("utf-8"))
            | "ParseOrders" >> beam.ParDo(ParseOrders())
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                table=BIGQUERY_TABLE,
                schema=("order_id:STRING,user_id:INTEGER,product_id:INTEGER,"
                        "quantity:INTEGER,price:FLOAT,timestamp:FLOAT"),
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == "__main__":
    run()