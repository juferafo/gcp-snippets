PROJECT=wave27-sellbytel-fejuan
BUCKET=fejuan-cases
REGION=us-central1

python main.py \
  --region $REGION \
  --input gs://dataflow-samples/shakespeare/kinglear.txt \
  --output gs://$BUCKET/results/outputs \
  --runner DataflowRunner \
  --project $PROJECT \
  --temp_location gs://$BUCKET/tmp/
