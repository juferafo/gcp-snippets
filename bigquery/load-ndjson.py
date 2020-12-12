"""
This script is designed to load .ndjson data into BigQuery

TO BE DONE BY THE DEVELOPER:

1 - Modify the FILENAME
2 - Modify the DATASET_ID
3 - Modify the TABLE_ID
4 - Modify the schema of the table according to your needs
"""

from google.cloud import bigquery

filename   = "FILENAME"
dataset_id = "DATASET_ID"
table_id   = "TABLE_ID"

client = bigquery.Client()
dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)

schema = [{"mode": "REQUIRED",
           "name": "id",
           "type": "INTEGER"},
          {"mode": "NULLABLE",
           "name": "name",
           "type": "STRING"},
          {"mode": "NULLABLE",
           "name": "color",
           "type": "STRING"}]

job_config = bigquery.LoadJobConfig()
job_config.autodetect = False
job_config.encoding = 'UTF-8'
job_config.schema = schema
job_config.WriteDisposition = bigquery.WriteDisposition.WRITE_APPEND
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
job_config.schema_update_options = [bigquery.job.SchemaUpdateOption.ALLOW_FIELD_ADDITION]

with open(filename, "rb") as source_file:
    print("File read")
    job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

job.result()

print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))
