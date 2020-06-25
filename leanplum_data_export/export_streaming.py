import csv
import json
import logging
import os
import re
import tempfile
from pathlib import Path
from typing import Dict, List

import boto3

from .base_exporter import BaseLeanplumExporter


class StreamingLeanplumExporter(BaseLeanplumExporter):

    DATA_TYPES = [
        "eventparameters", "events", "experiments", "sessions", "states", "userattributes"
    ]

    def __init__(self, project):
        super().__init__(project)
        self.s3_client = boto3.client("s3")

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

    def write_to_gcs(self, file_path: Path, data_type: str, bucket: str,
                     prefix: str, version: str, date: str) -> None:
        """
        Write file to GCS bucket so it can be transformed and loaded into Bigquery later
        This is also to circumvent the 1500 load job limit per table per day in Bigquery
        """
        gcs_path = os.path.join(self.get_gcs_prefix(prefix, version, date),
                                data_type, file_path.name)

        logging.info(f"Uploading {file_path.name} to gs://{gcs_path}")
        blob = self.gcs_client.bucket(bucket).blob(gcs_path)
        blob.upload_from_filename(str(file_path))

    def write_to_csv(self, csv_writers: Dict[str, csv.DictWriter], session_data: Dict,
                     schemas: Dict[str, List[str]]) -> None:
        for user_attribute in self.extract_user_attributes(session_data):
            csv_writers["userattributes"].writerow(user_attribute)

        for state in self.extract_states(session_data):
            csv_writers["states"].writerow(state)

        for experiment in self.extract_experiments(session_data):
            csv_writers["experiments"].writerow(experiment)

        csv_writers["sessions"].writerow(
            self.extract_session(session_data, schemas["sessions"]))

        events, event_parameters = self.extract_events(session_data)
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

    def export(self, date: str, s3_bucket: str, gcs_bucket: str, prefix: str, dataset: str,
               table_prefix: str, version: str) -> None:
        schemas = {data_type: [field["name"] for field in self.parse_schema(data_type)]
                   for data_type in self.DATA_TYPES}

        data_file_keys = self.get_files(date, s3_bucket, prefix)

        self.delete_gcs_prefix(self.gcs_client.bucket(gcs_bucket),
                               self.get_gcs_prefix(prefix, version, date))

        # Transform data file into csv for each data type and then save to GCS
        for key in data_file_keys:
            with tempfile.TemporaryDirectory() as data_dir:
                csv_file_paths = self.transform_data_file(key, schemas, data_dir, s3_bucket)

                for data_type, csv_file_path in csv_file_paths.items():
                    self.write_to_gcs(csv_file_path, data_type, gcs_bucket, prefix, version, date)

        self.create_external_tables(gcs_bucket, prefix, date, self.DATA_TYPES,
                                    self.TMP_DATASET, dataset, table_prefix, version)
        self.delete_existing_data(dataset, table_prefix, self.DATA_TYPES, version, date)
        self.load_tables(self.TMP_DATASET, dataset, table_prefix, self.DATA_TYPES, version, date)
        self.drop_external_tables(self.TMP_DATASET, dataset, table_prefix,
                                  self.DATA_TYPES, version, date)
