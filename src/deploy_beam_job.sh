export PROJECT=treinamento-254613

python ./src/dataflow/main.py \
  --runner DataflowRunner \
  --project $PROJECT \
  --temp_location gs://$PROJECT/tmp/ \
  --staging_location gs://$PROJECT/staging/ \
  --region us-central1