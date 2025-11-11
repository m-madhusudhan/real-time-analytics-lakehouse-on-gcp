import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv, io

class ParseCSV(beam.DoFn):
    def process(self, element):
        for row in csv.DictReader(io.StringIO(element)):
            row["order_id"] = row["order_id"]
            row["user_id"] = int(row["user_id"])
            row["product_id"] = int(row["product_id"])
            row["quantity"] = int(row["quantity"])
            row["price"] = float(row["price"])
            row["order_date"] = row["order_date"]
            yield row


def run():
    project_id = "qwiklabs-gcp-01-cffc452c1796"
    bucket_name = f"gs://{project_id}-bucket"

    opts = PipelineOptions(
        project=project_id,
        region="us-central1",
        temp_location=f"{bucket_name}/temp",
        job_name="batch-orders-load"
    )

    with beam.Pipeline(options=opts) as p:
        (
            p
            | "ReadFile" >> beam.io.ReadFromText(f"{bucket_name}/input/historical_orders.csv", skip_header_lines=1)
            | "ParseCSV" >> beam.ParDo(ParseCSV())
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                table=f"{project_id}:ecommerce.orders_batch",
                schema="order_id:STRING,user_id:INTEGER,product_id:INTEGER,quantity:INTEGER,price:FLOAT,order_date:TIMESTAMP",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == "__main__":
    run()