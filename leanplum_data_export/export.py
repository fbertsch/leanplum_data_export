import csv
import json
import logging
import os
import re
import tempfile
from pathlib import Path
from typing import Dict, List, Set

import boto3
from google.cloud import bigquery, exceptions, storage

from leanplum_data_export import data_parser


class LeanplumExporter(object):
    TMP_DATASET = "tmp"
    DROP_COLS = {"sessions": {"lat", "lon"}}
    SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "schemas", "")
    PARTITION_FIELD = "load_date"
    DATA_TYPES = [
        "eventparameters", "events", "experiments", "sessions", "states", "userattributes"
    ]
    FILE_HISTORY_PREFIX = "file_history"

    def __init__(self, project):
        self.bq_client = bigquery.Client(project=project)
        self.gcs_client = storage.Client(project=project)
        self.s3_client = boto3.client("s3")

    def export(self, date: str, s3_bucket: str, gcs_bucket: str, prefix: str, dataset: str,
               table_prefix: str, version: str, clean: bool) -> None:
        schemas = {data_type: [field["name"] for field in self.parse_schema(data_type)]
                   for data_type in self.DATA_TYPES}

        data_file_keys = self.get_files(date, s3_bucket, prefix)

        if clean:
            self.delete_gcs_prefix(self.gcs_client.bucket(gcs_bucket),
                                   self.get_gcs_prefix(prefix, version, date))

        file_history = self.get_previously_imported_files(gcs_bucket, prefix, version, date)

        # Transform data file into csv for each data type and then save to GCS
        for key in data_file_keys:
            data_file_name = os.path.basename(key)
            if data_file_name in file_history:
                logging.info(f"Skipping export for {data_file_name}")
                continue

            with tempfile.TemporaryDirectory() as data_dir:
                csv_file_paths = self.transform_data_file(key, schemas, data_dir, s3_bucket)

                for data_type, csv_file_path in csv_file_paths.items():
                    self.write_to_gcs(csv_file_path, data_type, gcs_bucket, prefix, version, date)

            with tempfile.NamedTemporaryFile() as empty_file:
                self.write_to_gcs(Path(empty_file.name), self.FILE_HISTORY_PREFIX,
                                  gcs_bucket, prefix, version, date, file_name=data_file_name)

        self.create_external_tables(gcs_bucket, prefix, date, self.DATA_TYPES,
                                    self.TMP_DATASET, dataset, table_prefix, version)
        self.delete_existing_data(dataset, table_prefix, self.DATA_TYPES, version, date)
        self.load_tables(self.TMP_DATASET, dataset, table_prefix, self.DATA_TYPES, version, date)
        self.drop_external_tables(self.TMP_DATASET, dataset, table_prefix,
                                  self.DATA_TYPES, version, date)

    def get_files(self, date: str, bucket: str, prefix: str, max_keys: int = None) -> List[str]:
        """
        Get the s3 keys of the data files in the given bucket
        """
        max_keys = {} if max_keys is None else {"MaxKeys": max_keys}  # for testing pagination
        filename_re = re.compile(r"^.*/\d{8}/export-.*-output-([0-9]+)$")
        data_file_keys = []

        continuation_token = {}  # value used for pagination
        while True:
            object_list = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=os.path.join(prefix, date, "export-"),
                **continuation_token,
                **max_keys,
            )
            data_file_keys.extend([content["Key"] for content in object_list["Contents"]
                                   if filename_re.fullmatch(content["Key"])])

            if not object_list["IsTruncated"]:
                break

            continuation_token["ContinuationToken"] = object_list["NextContinuationToken"]

        return data_file_keys

    def get_previously_imported_files(self, bucket: str, prefix: str,
                                      version: str, date: str) -> Set[str]:
        """
        Get file names of data files that have already been imported into GCS
        """
        blobs = self.gcs_client.list_blobs(
            bucket,
            prefix=self.get_gcs_prefix(prefix, version, date, self.FILE_HISTORY_PREFIX)
        )
        file_names = []
        for page in blobs.pages:
            for blob in page:
                file_names.append(os.path.basename(blob.name))

        return set(file_names)

    def write_to_gcs(self, file_path: Path, data_type: str, bucket: str,
                     prefix: str, version: str, date: str, file_name: str = None) -> None:
        """
        Write file to GCS bucket
        If a Path is given as file_path, the file is uploaded
        """
        if file_name is None:
            file_name = file_path.name
        gcs_path = os.path.join(self.get_gcs_prefix(prefix, version, date, data_type), file_name)

        logging.info(f"Uploading {file_name} to gs://{gcs_path}")
        blob = self.gcs_client.bucket(bucket).blob(gcs_path)
        blob.upload_from_filename(str(file_path))

    def write_to_csv(self, csv_writers: Dict[str, csv.DictWriter], session_data: Dict,
                     schemas: Dict[str, List[str]]) -> None:
        for user_attribute in data_parser.extract_user_attributes(session_data):
            csv_writers["userattributes"].writerow(user_attribute)

        for state in data_parser.extract_states(session_data):
            csv_writers["states"].writerow(state)

        for experiment in data_parser.extract_experiments(session_data):
            csv_writers["experiments"].writerow(experiment)

        csv_writers["sessions"].writerow(
            data_parser.extract_session(session_data, schemas["sessions"]))

        events, event_parameters = data_parser.extract_events(session_data)
        for event in events:
            csv_writers["events"].writerow(event)
        for event_parameter in event_parameters:
            csv_writers["eventparameters"].writerow(event_parameter)

    def transform_data_file(self, data_file_key: str, schemas: Dict[str, List[str]],
                            data_dir: str, bucket: str) -> Dict[str, Path]:
        """
        Get data file contents and convert from JSON to CSV for each data type and
        return paths to the files.
        The JSON data file is not in a format that can be loaded into bigquery.
        """
        logging.info(f"Exporting {data_file_key}")

        # downloading the entire file at once is much faster than using boto3 s3 streaming
        self.s3_client.download_file(bucket, data_file_key, os.path.join(data_dir, "data.ndjson"))

        file_id = "-".join(data_file_key.split("-")[2:])
        csv_file_paths = {data_type: Path(os.path.join(data_dir, f"{data_type}-{file_id}.csv"))
                          for data_type in self.DATA_TYPES}
        csv_files = {data_type: open(file_path, "w")
                     for data_type, file_path in csv_file_paths.items()}
        try:
            csv_writers = {data_type: csv.DictWriter(csv_files[data_type], schemas[data_type],
                                                     extrasaction="ignore")
                           for data_type in self.DATA_TYPES}
            for csv_writer in csv_writers.values():
                csv_writer.writeheader()
            with open(os.path.join(data_dir, "data.ndjson")) as f:
                for line in f:
                    session_data = json.loads(line)
                    self.write_to_csv(csv_writers, session_data, schemas)
        finally:
            for csv_file in csv_files.values():
                csv_file.close()

        return csv_file_paths

    def delete_gcs_prefix(self, bucket, prefix):
        blobs = self.gcs_client.list_blobs(bucket, prefix=prefix)

        for page in blobs.pages:
            bucket.delete_blobs(list(page))

    def create_external_tables(self, bucket_name, prefix, date, tables,
                               ext_dataset, dataset, table_prefix, version):
        """
        Create external tables using CSVs in GCS as the data source
        """
        gcs_loc = f"gs://{bucket_name}/{self.get_gcs_prefix(prefix, version, date)}"
        dataset_ref = self.bq_client.dataset(ext_dataset)

        for leanplum_name in tables:
            table_name = self.get_table_name(table_prefix, leanplum_name, version, date, dataset)
            logging.info(f"Creating external table {ext_dataset}.{table_name}")

            table_ref = bigquery.TableReference(dataset_ref, table_name)
            table = bigquery.Table(table_ref)

            self.bq_client.delete_table(table, not_found_ok=True)

            schema = [
                bigquery.SchemaField(
                    field["name"],
                    field_type=field.get("type", "STRING"),
                    mode=field.get("mode", "NULLABLE"),
                )
                for field in self.parse_schema(leanplum_name)
            ]

            external_config = bigquery.ExternalConfig('CSV')
            external_config.source_uris = [os.path.join(gcs_loc, leanplum_name, "*")]
            external_config.schema = schema
            # there are rare cases of corrupted values that should be ignored instead of failing
            external_config.max_bad_records = 100
            external_config.options.skip_leading_rows = 1
            external_config.options.allow_quoted_newlines = True

            table.external_data_configuration = external_config

            self.bq_client.create_table(table)

    def delete_existing_data(self, dataset, table_prefix, tables, version, date):
        """
        Delete existing data in the target table partition
        """
        for table in tables:
            table_name = self.get_table_name(table_prefix, table, version)

            delete_sql = (
                f"DELETE FROM `{dataset}.{table_name}` "
                f"WHERE {self.PARTITION_FIELD} = PARSE_DATE('%Y%m%d', '{date}')")

            logging.info(f"Deleting data from {dataset}.{table_name}")
            logging.info(delete_sql)
            self.bq_client.query(delete_sql)

    def load_tables(self, ext_dataset, dataset, table_prefix, tables, version, date):
        """
        Load data from external tables into final tables using SELECT statement
        """
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
                f"SELECT * {drop_clause}, PARSE_DATE('%Y%m%d', '{date}') AS {self.PARTITION_FIELD} "
                f"FROM `{ext_dataset}.{ext_table_name}`")

            if not self.get_table_exists(destination_table):
                sql = (
                    f"CREATE TABLE `{dataset}.{table_name}` "
                    f"PARTITION BY {self.PARTITION_FIELD} AS {select_sql}")
            else:
                sql = f"INSERT INTO `{dataset}.{table_name}` {select_sql}"

            logging.info((
                f"Inserting into native table {dataset}.{table_name} "
                f"from {ext_dataset}.{ext_table_name}"))
            logging.info(sql)

            job = self.bq_client.query(sql)
            job.result()

    def drop_external_tables(self, ext_dataset, dataset, table_prefix, tables, version, date):
        """
        Delete temporary tables used for loading data into final tables
        """
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

    def parse_schema(self, data_type):
        try:
            with open(os.path.join(
                    self.SCHEMA_DIR, f"{data_type}.schema.json"), "r") as schema_file:
                return [field for field in json.load(schema_file)]
        except FileNotFoundError:
            raise ValueError(f"Unrecognized table name encountered: {data_type}")

    @staticmethod
    def get_gcs_prefix(prefix, version, date, data_type=None):
        if data_type is None:
            return os.path.join(prefix, f"v{version}", date, "")
        else:
            return os.path.join(prefix, f"v{version}", date, data_type, "")
