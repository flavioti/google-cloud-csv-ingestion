gcloud beta dataflow jobs list

export PROJECT=treinamento-254613
export BUCKET=gs://dotz-exam

python /home/semantix/sources/flavio/github/google-cloud-csv-ingestion/src/dataflow/main2.py \
  --runner DataflowRunner \
  --project $PROJECT \
  --temp_location gs://$BUCKET/tmp/