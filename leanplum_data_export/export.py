import json
import logging
import os
import re
import requests
import time

from google.cloud import bigquery, exceptions, storage
from pathlib import Path


class LeanplumExporter(object):

    FINISHED_STATE = "FINISHED"
    DEFAULT_SLEEP_SECONDS = 10
    DEFAULT_EXPORT_FORMAT = "csv"
    FILENAME_RE = (r"^https://leanplum_export.storage.googleapis.com"
                   "/export-.*-output([a-z0-9]+)-([0-9]+)$")
    TMP_DATASET = "tmp"
    DROP_COLS = {"sessions": {"lat", "lon"}}
    SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "schemas/")

    def __init__(self, app_id, client_key):
        self.app_id = app_id
        self.bq_client_key = client_key
        self.filename_re = re.compile(LeanplumExporter.FILENAME_RE)
        self.partition_field = "load_date"

    def export(self, date, bucket, prefix, dataset, table_prefix,
               version, project, export_format=DEFAULT_EXPORT_FORMAT):
        self.bq_client = bigquery.Client(project=project)
        self.gcs_client = storage.Client()

        job_id = self.init_export(date, export_format)
        file_uris = self.get_files(job_id)
        tables = self.save_files(file_uris, bucket, prefix, date, export_format, version)
        self.create_external_tables(bucket, prefix, date, tables, self.TMP_DATASET,
                                    dataset, table_prefix, version)
        self.delete_existing_data(dataset, table_prefix, tables, version, date)
        self.load_tables(self.TMP_DATASET, dataset, table_prefix, tables, version, date)
        self.drop_external_tables(self.TMP_DATASET, dataset, table_prefix, tables, version, date)

    def init_export(self, date, export_format):
        export_init_url = (f"http://www.leanplum.com/api"
                           f"?appId={self.app_id}"
                           f"&clientKey={self.bq_client_key}"
                           f"&apiVersion=1.0.6"
                           f"&action=exportData"
                           f"&startDate={date}"
                           f"&exportFormat={export_format}")

        logging.info("Export Init URL: " + export_init_url)
        response = requests.get(export_init_url)
        response.raise_for_status()

        # Will hard fail if not present
        return response.json()["response"][0]["jobId"]

    def get_files(self, job_id, sleep_time=DEFAULT_SLEEP_SECONDS):
        export_retrieve_url = (f"http://www.leanplum.com/api?"
                               f"appId={self.app_id}"
                               f"&clientKey={self.bq_client_key}"
                               f"&apiVersion=1.0.6"
                               f"&action=getExportResults"
                               f"&jobId={job_id}")

        logging.info("Export URL: " + export_retrieve_url)
        loading = True

        while(loading):
            response = requests.get(export_retrieve_url)
            response.raise_for_status()

            state = response.json()['response'][0]['state']
            if (state == LeanplumExporter.FINISHED_STATE):
                logging.info("Export Ready")
                loading = False
            else:
                logging.info("Waiting for export to finish...")
                time.sleep(sleep_time)

        return response.json()['response'][0]['files']

    def save_files(self, file_uris, bucket_name, prefix, date, export_format, version):
        # Avoid calling storage.buckets.get since we don't need bucket metadata
        # https://github.com/googleapis/google-cloud-python/issues/3433
        bucket = self.gcs_client.bucket(bucket_name)
        datatypes = set()

        version_str = f"v{version}"
        if prefix:
            prefix = self.add_slash_if_not_present(prefix) + version_str
        else:
            prefix = version_str

        prefix += f"/{date}"
        self.delete_gcs_prefix(bucket, prefix)

        for uri in file_uris:
            logging.info(f"Retrieving URI {uri}")

            parsed = self.filename_re.fullmatch(uri)
            if parsed is None:
                raise Exception((f"Expected uri matching {LeanplumExporter.FILENAME_RE}"
                                 f", but got {uri}"))

            datatype, index = parsed.group(1), parsed.group(2)
            local_filename = f"{datatype}/{index}.{export_format}"
            datatypes |= set([datatype])

            f = Path(local_filename)
            base_dir = Path(datatype)
            base_dir.mkdir(parents=True, exist_ok=True)

            with requests.get(uri, stream=True) as r:
                r.raise_for_status()
                with f.open("wb") as opened:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            opened.write(chunk)

            logging.info(f"Uploading to gs://{bucket_name}/{prefix}/{local_filename}")
            blob = bucket.blob(f"{prefix}/{local_filename}")
            blob.upload_from_filename(local_filename)

            f.unlink()
            base_dir.rmdir()

        return datatypes

    def delete_gcs_prefix(self, bucket, prefix):
        blobs = self.gcs_client.list_blobs(bucket, prefix=prefix)

        for page in blobs.pages:
            bucket.delete_blobs(list(page))

    def create_external_tables(self, bucket_name, prefix, date, tables,
                               ext_dataset, dataset, table_prefix, version):
        gcs_loc = f"gs://{bucket_name}/{prefix}/v{version}/{date}"
        dataset_ref = self.bq_client.dataset(ext_dataset)

        for leanplum_name in tables:
            table_name = self.get_table_name(table_prefix, leanplum_name, version, date, dataset)
            logging.info(f"Creating external table {ext_dataset}.{table_name}")

            table_ref = bigquery.TableReference(dataset_ref, table_name)
            table = bigquery.Table(table_ref)

            self.bq_client.delete_table(table, not_found_ok=True)

            try:
                schema_file_path = [
                    os.path.join(self.SCHEMA_DIR, f) for f
                    in os.listdir(self.SCHEMA_DIR)
                    if f.split(".")[0] == leanplum_name
                ][0]
            except IndexError:
                raise Exception(f"Unrecognized table name encountered: {leanplum_name}")

            external_config = bigquery.ExternalConfig('CSV')
            external_config.source_uris = [f"{gcs_loc}/{leanplum_name}/*"]
            external_config.schema = self.parse_schema(schema_file_path)
            external_config.options.allow_quoted_newlines = True

            table.external_data_configuration = external_config

            self.bq_client.create_table(table)

    def delete_existing_data(self, dataset, table_prefix, tables, version, date):
        for table in tables:
            table_name = self.get_table_name(table_prefix, table, version)

            delete_sql = (
                f"DELETE FROM `{dataset}.{table_name}` "
                f"WHERE {self.partition_field} = PARSE_DATE('%Y%m%d', '{date}')")

            logging.info(f"Deleting data from {dataset}.{table_name}")
            logging.info(delete_sql)
            self.bq_client.query(delete_sql)

    def load_tables(self, ext_dataset, dataset, table_prefix, tables, version, date):
        destination_dataset = self.bq_client.dataset(dataset)

        for table in tables:
            ext_table_name = self.get_table_name(table_prefix, table, version, date, dataset)
            table_name = self.get_table_name(table_prefix, table, version)

            destination_table = bigquery.TableReference(destination_dataset, table_name)

            drop_cols = self.DROP_COLS.get(table, set())
            drop_clause = ""
            if drop_cols:
                drop_clause = f"EXCEPT ({','.join(sorted(drop_cols))})"

            select_sql = (
                f"SELECT * {drop_clause}, PARSE_DATE('%Y%m%d', '{date}') AS {self.partition_field} "
                f"FROM `{ext_dataset}.{ext_table_name}`")

            if not self.get_table_exists(destination_table):
                sql = (
                    f"CREATE TABLE `{dataset}.{table_name}` "
                    f"PARTITION BY {self.partition_field} AS {select_sql}")
            else:
                sql = f"INSERT INTO `{dataset}.{table_name}` {select_sql}"

            logging.info((
                f"Inserting into native table {dataset}.{table_name} "
                f"from {ext_dataset}.{ext_table_name}"))
            logging.info(sql)

            job = self.bq_client.query(sql)
            job.result()

    def drop_external_tables(self, ext_dataset, dataset, table_prefix, tables, version, date):
        dataset_ref = self.bq_client.dataset(ext_dataset)

        for leanplum_name in tables:
            table_name = self.get_table_name(table_prefix, leanplum_name, version, date, dataset)
            table_ref = bigquery.TableReference(dataset_ref, table_name)
            table = bigquery.Table(table_ref)

            logging.info(f"Dropping table {ext_dataset}.{table_name}")

            self.bq_client.delete_table(table)

    def get_table_exists(self, table):
        try:
            table = self.bq_client.get_table(table)
            return True
        except exceptions.NotFound:
            return False

    def get_table_name(self, table_prefix, leanplum_name, version, date=None, dataset_prefix=None):
        if table_prefix:
            table_prefix += "_"
        else:
            table_prefix = ""

        name = f"{table_prefix}{leanplum_name}_v{version}"
        if dataset_prefix is not None:
            name = f"{dataset_prefix}_{name}"
        if date is not None:
            name += f"_{date}"

        return name

    def add_slash_if_not_present(self, val):
        if not val.endswith("/"):
            val = f"{val}/"
        return val

    def parse_schema(self, schema_file_path):
        with open(schema_file_path) as schema_file:
            fields = json.load(schema_file)
        return [
            bigquery.SchemaField(
                field["name"],
                field_type=field.get("type", "STRING"),
                mode=field.get("mode", "NULLABLE"),
            )
            for field in fields
        ]
