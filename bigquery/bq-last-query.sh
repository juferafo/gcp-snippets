#!/bin/bash

# This script prints the job ID and the information related to the last query run in BigQuery

LAST_JOB_ID=$(bq ls -j -a | grep query | head -1 | awk '{print $1}')

echo Job ID of the last query $LAST_JOB_ID
echo

echo Job information of $LAST_JOB_ID
echo
bq show --format=prettyjson -j $LAST_JOB_ID
