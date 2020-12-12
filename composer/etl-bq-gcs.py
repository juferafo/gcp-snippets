#!/usr/bin/env python

"""
This script is designed to schedule the following ETL in Airflow:

1 - make_bq_dataset -> it creates a BQ temporary dataset
2 - bq_recent_questions_query -> it executes a query and save the results into a BQ table
3 - export_questions_to_gcs -> it exports the results into GCS
4 - delete_bq_dataset -> it deletes the temporary BQ dataset


TO BE DONE BY THE DEVELOPER:

1 - Modify the PROJECT_ID
2 - Modify the BUCKET_ID
3 - Modify the DATASET_ID
"""

import datetime
from airflow import models
from airflow.contrib.operators import bigquery_get_data
from airflow.contrib.operators import bigquery_operator
from airflow.contrib.operators import bigquery_to_gcs
from airflow.operators import bash_operator
from airflow.utils import trigger_rule


# Project  ID
project_id = "PROJECT_ID"

# BQ dataset variables
bq_dataset_id = 'DATASET_ID'
bq_table_id = bq_dataset_id + '.questions'
bq_most_popular_table_id = bq_dataset_id + '.most_popular'
output_file = 'gs://BUCKET_ID/questions.csv'

# Datetime variables
max_query_date = '2018-02-01'
min_query_date = '2018-01-01'
yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

# DAG
# default arguments
default_dag_args = {
    'start_date': yesterday,
    'project_id': "PROJECT_ID"
}

with models.DAG(
        'composer_sample_bq_pipe',
        schedule_interval=datetime.timedelta(minutes=30),
        default_args=default_dag_args) as dag:

    # Create BigQuery output dataset.
    make_bq_dataset = bash_operator.BashOperator(
        task_id='make_bq_dataset',
        # Executing 'bq' command requires Google Cloud SDK which comes
        # preinstalled in Cloud Composer.
        bash_command='bq ls {} || bq mk {}'.format(
            bq_dataset_id, bq_dataset_id))

    # Query recent StackOverflow questions.
    bq_recent_questions_query = bigquery_operator.BigQueryOperator(
        task_id='bq_recent_questions_query',
        bql="""
        SELECT owner_display_name, title, view_count
        FROM `bigquery-public-data.stackoverflow.posts_questions`
        WHERE creation_date < CAST('{max_date}' AS TIMESTAMP)
            AND creation_date >= CAST('{min_date}' AS TIMESTAMP)
        ORDER BY view_count DESC
        LIMIT 100
        """.format(max_date=max_query_date, min_date=min_query_date),
        use_legacy_sql=False,
        destination_dataset_table=bq_table_id)

    # Export query result to Cloud Storage.
    export_questions_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_questions_to_gcs',
        source_project_dataset_table=bq_table_id,
        destination_cloud_storage_uris=[output_file],
        export_format='CSV')

    # Delete BigQuery dataset
    delete_bq_dataset = bash_operator.BashOperator(
        task_id='delete_bq_dataset',
        bash_command='bq rm -r -f %s' % bq_dataset_id,
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)


    # Define DAG dependencies.
    make_bq_dataset >> bq_recent_questions_query >> export_questions_to_gcs >> delete_bq_dataset
