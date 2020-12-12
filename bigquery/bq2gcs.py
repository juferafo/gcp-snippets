def bq2gcs(request):
    """
    This method is designed to backup data from BQ into GCS
    It can be used in a Cloud Function to trigger periodic runs with Cloud Scheduler

    TO BE DONE BY THE DEVELOPER:

    1 - Modify the PROJECT_ID
    2 - Modify the BUCKET_ID
    3 - Modify the DATASET_ID
    4 - Modify the TABLE_ID
    """

    from google.cloud import bigquery
    import datetime

    client = bigquery.Client()
    bucket_name = "gs://BUCKET_ID/bq-backup"
    project = "PROJECT_ID"
    dataset_id = "DATASET_ID"
    table_id = "TABLE_ID"
    # backups are saved with timestamp for tracking
    output = "backup-{}.csv".format(datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))

    destination_uri = "{}/{}".format(bucket_name, output)
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_id)

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        # Location must match that of the source table.
        location="US",
    )  # API request
    extract_job.result()  # Waits for job to complete.

    return "Done"

bq2gcs("dummy")
