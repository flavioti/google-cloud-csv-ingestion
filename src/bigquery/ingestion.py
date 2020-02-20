import os.path

from google.cloud import bigquery as bq


class AnaliseExploratoria():

    @property
    def data_dir(self): return self._data_dir

    @data_dir.setter
    def data_dir(self, value): self._data_dir = value

    def __init__(self):
        self.data_dir = os.path.dirname(__file__) + "/../dataflow/data/"

    def processar(self, client, load_data):
        """ Teste """
        dataset_id = "treinamento-254613.dotzexam"
        self.load_price_quote(client, dataset_id, load_data)
        self.load_bill_of_materials(client, dataset_id, load_data)
        self.load_comp_boss(client, dataset_id, load_data)

    def load_price_quote(self, client, dataset_id, load_data):
        table_id = "{}.{}".format(dataset_id, "price_quote")

        schema = [
            bq.SchemaField("tube_assembly_id", "STRING", mode="NULLABLE"),
            bq.SchemaField("supplier", "STRING", mode="NULLABLE"),
            bq.SchemaField("quote_date", "DATE", mode="NULLABLE"),
            bq.SchemaField("annual_usage", "INTEGER", mode="NULLABLE"),
            bq.SchemaField("min_order_quantity", "INTEGER", mode="NULLABLE"),
            bq.SchemaField("bracket_pricing", "BOOLEAN", mode="NULLABLE"),
            bq.SchemaField("quantity", "INTEGER", mode="NULLABLE"),
            bq.SchemaField("cost", "FLOAT", mode="NULLABLE")]

        client.delete_table(table=table_id, not_found_ok=True)
        table = bq.Table(table_id, schema=schema)
        client.create_table(table, exists_ok=True)

        if load_data:
            dataset_ref = client.dataset(dataset_id)
            table_ref = dataset_ref.table(table_id)

            job_config = bq.LoadJobConfig()
            job_config.source_format = bq.SourceFormat.CSV
            job_config.skip_leading_rows = 1
            job_config.autodetect = False
            job_config.schema_update_options = []
            job_config.max_bad_records = 0

            with open(self.data_dir + "price_quote.csv", "rb") as source_file:
                job = client.load_table_from_file(
                    file_obj=source_file, destination=table_id, job_config=job_config)
            job.result()
            print("Loaded {} rows into {}:{}.".format(
                job.output_rows, dataset_id, table_id))

    def load_bill_of_materials(self, client, dataset_id, load_data):

        table_id = "{}.{}".format(dataset_id, "bill_of_materials")

        schema = [
            bq.SchemaField("tube_assembly_id", "STRING", mode="REQUIRED"),
            bq.SchemaField("component_id_1", "STRING", mode="REQUIRED"),
            bq.SchemaField("quantity_1", "STRING", mode="REQUIRED"),
            bq.SchemaField("component_id_2", "STRING", mode="REQUIRED"),
            bq.SchemaField("quantity_2", "STRING", mode="REQUIRED"),
            bq.SchemaField("component_id_3", "STRING", mode="REQUIRED"),
            bq.SchemaField("quantity_3", "STRING", mode="REQUIRED"),
            bq.SchemaField("component_id_4", "STRING", mode="REQUIRED"),
            bq.SchemaField("quantity_4", "STRING", mode="REQUIRED"),
            bq.SchemaField("component_id_5", "STRING", mode="REQUIRED"),
            bq.SchemaField("quantity_5", "STRING", mode="REQUIRED"),
            bq.SchemaField("component_id_6", "STRING", mode="REQUIRED"),
            bq.SchemaField("quantity_6", "STRING", mode="REQUIRED"),
            bq.SchemaField("component_id_7", "STRING", mode="REQUIRED"),
            bq.SchemaField("quantity_7", "STRING", mode="REQUIRED"),
            bq.SchemaField("component_id_8", "STRING", mode="REQUIRED"),
            bq.SchemaField("quantity_8", "STRING", mode="REQUIRED")]

        client.delete_table(table=table_id, not_found_ok=True)
        table = bq.Table(table_id, schema=schema)
        client.create_table(table, exists_ok=True)

        if load_data:
            dataset_ref = client.dataset(dataset_id)
            table_ref = dataset_ref.table(table_id)

            job_config = bq.LoadJobConfig()
            job_config.source_format = bq.SourceFormat.CSV
            job_config.skip_leading_rows = 1
            job_config.autodetect = False
            job_config.schema_update_options = []
            job_config.max_bad_records = 0

            with open(self.data_dir + "bill_of_materials.csv", "rb") as source_file:
                job = client.load_table_from_file(
                    file_obj=source_file, destination=table_id, job_config=job_config)
            job.result()
            print("Loaded {} rows into {}:{}.".format(
                job.output_rows, dataset_id, table_id))

    def load_comp_boss(self, client, dataset_id, load_data):

        table_id = "{}.{}".format(dataset_id, "comp_boss")

        schema = [
            bq.SchemaField("component_id", "STRING", mode="NULLABLE"),
            bq.SchemaField("component_type_id", "STRING", mode="NULLABLE"),
            bq.SchemaField("type", "STRING", mode="NULLABLE"),
            bq.SchemaField("connection_type_id", "STRING", mode="NULLABLE"),
            bq.SchemaField("outside_shape", "STRING", mode="NULLABLE"),
            bq.SchemaField("base_type", "STRING", mode="NULLABLE"),
            bq.SchemaField("height_over_tube", "STRING", mode="NULLABLE"),
            bq.SchemaField("bolt_pattern_long", "STRING", mode="NULLABLE"),
            bq.SchemaField("bolt_pattern_wide", "STRING", mode="NULLABLE"),
            bq.SchemaField("groove", "STRING", mode="NULLABLE"),
            bq.SchemaField("base_diameter", "STRING", mode="NULLABLE"),
            bq.SchemaField("shoulder_diameter", "STRING", mode="NULLABLE"),
            bq.SchemaField("unique_feature", "STRING", mode="NULLABLE"),
            bq.SchemaField("orientation", "STRING", mode="NULLABLE"),
            bq.SchemaField("weight", "STRING", mode="NULLABLE")]

        client.delete_table(table=table_id, not_found_ok=True)
        table = bq.Table(table_id, schema=schema)
        client.create_table(table, exists_ok=True)

        if load_data:
            dataset_ref = client.dataset(dataset_id)
            table_ref = dataset_ref.table(table_id)

            job_config = bq.LoadJobConfig()
            job_config.source_format = bq.SourceFormat.CSV
            job_config.skip_leading_rows = 1
            job_config.autodetect = False
            job_config.schema_update_options = []
            job_config.max_bad_records = 0

            with open(self.data_dir + "comp_boss.csv", "rb") as source_file:
                job = client.load_table_from_file(
                    file_obj=source_file, destination=table_id, job_config=job_config)
            job.result()
            print("Loaded {} rows into {}:{}.".format(
                job.output_rows, dataset_id, table_id))
