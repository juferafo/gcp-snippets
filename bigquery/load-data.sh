# This script loads the ./data.csv into the BQ table PROJECT_ID:DATASET_ID.TABLE_ID

BQ_PATH=$1

echo ./data file loaded into $BQ_PATH

bq load \
   --source_format=CSV \
   $BQ_PATH \
   ./data.csv \
   ./schema.json
