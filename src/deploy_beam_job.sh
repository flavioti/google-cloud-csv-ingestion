export PROJECT=treinamento-254613

python ./src/dataflow/main2.py \
  --runner DataflowRunner \
  --project $PROJECT \
  --temp_location gs://$PROJECT/tmp/ \
  --staging_location gs://$PROJECT/staging/ \
  --region us-central1