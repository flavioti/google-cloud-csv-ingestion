#!/bin/bash

# ENVIA ARQUIVOS PARA BUCKET NO GCP

# Ativa a configuração do projeto da minha conta pessoal
gcloud config configurations list
gcloud config configurations activate pessoal

# cria o bucket no GCP
gsutil mb gs://dotz-exam
gsutil versioning set on gs://dotz-exam

# Envia os arquivos para o Bucket
gsutil cp ./data/price_quote.csv ./data/bill_of_materials.csv ./data/comp_boss.csv  gs://dotz-exam/raw/
