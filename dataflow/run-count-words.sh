# Adapt the below parameters according to your needs

PROJECT=<PROJECT_ID>
BUCKET=<BUCKET_ID>
REGION=<REGION>

python count-words.py \
  --region $REGION \
  --input gs://dataflow-samples/shakespeare/kinglear.txt \
  --output gs://$BUCKET/results/outputs \
  --runner DataflowRunner \
  --project $PROJECT \
  --temp_location gs://$BUCKET/tmp/
