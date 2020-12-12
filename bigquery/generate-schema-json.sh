# This script generates the schema of the BQ table PROJECT_ID:DATASET_ID.TABLE_ID in json format

BQ_PATH=$1

bq show --format=prettyjson $BQ_PATH | jq '.schema.fields' > schema.json

echo Schema of table $BQ_PATH generated in ./schema.json
