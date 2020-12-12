"""
This script is designed to create a BigQuery scheduled query

TO BE DONE BY THE DEVELOPER:

1 - Provide the PROJECT_ID
2 - Provide the target DATASET_ID
3 - Provide the desired query
3 - Provide the service accoun namet
"""

from google.cloud import bigquery_datatransfer_v1
import google.protobuf.json_format
from datetime import datetime
from datetime import timedelta
import pytz


def create_scheduled_query(\
    project_id="PROJECT_ID",\
    dataset_id="DATASET_ID",\
    query_string="SELECT * FROM `bigquery-public-data.austin_crime.crime` LIMIT 1000",\
    dest_table="test",\
    write_disposition="WRITE_TRUNCATE"):

    client = bigquery_datatransfer_v1.DataTransferServiceClient()
    parent = client.project_path(project_id)

    now_dt = datetime(*((datetime.now() + timedelta(days=1)).timetuple())[:3])
    utc_start_dt = pytz.timezone("UTC").localize(now_dt).astimezone(pytz.UTC)
    schedule_hour = utc_start_dt.strftime('%H:%M')


    transfer_config = google.protobuf.json_format.ParseDict(
        {
            "destination_dataset_id": dataset_id,
            "display_name": dest_table+"UTC",
            "data_source_id": "scheduled_query",
            "params": {
                "query": query_string,
                "destination_table_name_template": dest_table,
                "write_disposition": write_disposition,
                "partitioning_field": "",
            },
            #"schedule": f"every 24 hours",
            "schedule": f"every day {schedule_hour}",
        },
        bigquery_datatransfer_v1.types.TransferConfig(),
    )

    return client.create_transfer_config(
        parent, transfer_config, service_account_name='service-account@PROJECT_ID.iam.gserviceaccount.com')

create_scheduled_query()
