"""
This script is designed to schedule periodic runs of the dataflow pipeline "py_file" in Airflow
For this purpose one can use as an example ./dataflow-count-words.py

TO BE DONE BY THE DEVELOPER:

1 - Set the Variables "bucket_path", "project_id", "gce_zone" and "gce_region" in Airflow
2 - Adapt the variable "py_file" to the path of your Apache Beam code
"""


import datetime

from airflow import models
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.utils.dates import days_ago

bucket_path = models.Variable.get("bucket_path")
project_id = models.Variable.get("project_id")
gce_zone = models.Variable.get("gce_zone")
gce_region = models.Variable.get("gce_region")

default_args = {
    # Tell airflow to start one day ago, so that it runs as soon as you upload it
    "start_date": days_ago(1),
    "dataflow_default_options": {
        "project": project_id,
        # Set to your region
        "region": gce_region,
        # Set to your zone
        "zone": gce_zone,
        # This is a subfolder for storing temporary files, like the staged pipeline job.
        "temp_location": bucket_path + "/tmp/",
        "input": "gs://dataflow-samples/shakespeare/kinglear.txt",
        "output": bucket_path + "/results/outputs",
    },
}

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
    # The id you will see in the DAG airflow page
    "composer_dataflow_dag",
    default_args=default_args,
    # The interval with which to schedule the DAG
    schedule_interval=datetime.timedelta(days=1),  # Override to match your needs
) as dag:

    start_template_job = DataFlowPythonOperator(
        task_id='dataflow_task',
        py_file=bucket_path + '/main.py',
        gcp_conn_id='google_cloud_default',
        dag=dag
    )

    """
    start_template_job = DataflowTemplateOperator(
        # The task id of your job
        task_id="dataflow_operator_transform_csv_to_bq",
        # The name of the template that you're using.
        # Below is a list of all the templates you can use.
        # For versions in non-production environments, use the subfolder 'latest'
        # https://cloud.google.com/dataflow/docs/guides/templates/provided-batch#gcstexttobigquery
        template="gs://dataflow-templates/latest/GCS_Text_to_BigQuery",
        # Use the link above to specify the correct parameters for your template.
        parameters={
            "javascriptTextTransformFunctionName": "transformCSVtoJSON",
            "JSONPath": bucket_path + "/jsonSchema.json",
            "javascriptTextTransformGcsPath": bucket_path + "/transformCSVtoJSON.js",
            "inputFilePattern": bucket_path + "/inputFile.txt",
            "outputTable": project_id + ":average_weather.average_weather",
            "bigQueryLoadingTemporaryDirectory": bucket_path + "/tmp/",
        },
    )
    """
