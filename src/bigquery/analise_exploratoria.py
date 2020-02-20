from google.cloud import bigquery as bq
from google.oauth2 import service_account

from ingestion import AnaliseExploratoria


def run():
    key_path = ".credentials/exame-ingestao-csv-key.json"
    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"])

    client = bq.Client(
        credentials=credentials,
        project=credentials.project_id)

    ae = AnaliseExploratoria()

    ae.processar(client, True)

    print("Processo de ingestão finalizado")


if __name__ == "__main__":
    run()
