import csv
import json
import logging
import os
import re
import tempfile
from pathlib import Path
from typing import Dict, List, Set, Union

import boto3

from .base_exporter import BaseLeanplumExporter


class StreamingLeanplumExporter(BaseLeanplumExporter):

    DATA_TYPES = [
        "eventparameters", "events", "experiments", "sessions", "states", "userattributes"
    ]
    FILE_HISTORY_PREFIX = "file_history"

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

    def write_to_gcs(self, file_path: Union[Path, str], data_type: str, bucket: str,
                     prefix: str, version: str, date: str) -> None:
        """
        Write file to GCS bucket
        If a Path is given as file_path, the file is uploaded
        If a string is given as file_path, an empty file is uploaded
        """
        file_name = file_path if isinstance(file_path, str) else file_path.name
        gcs_path = os.path.join(self.get_gcs_prefix(prefix, version, date, data_type), file_name)

        logging.info(f"Uploading {file_name} to gs://{gcs_path}")
        blob = self.gcs_client.bucket(bucket).blob(gcs_path)
        if isinstance(file_path, str):
            blob.upload_from_string("")
        else:
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
               table_prefix: str, version: str, clean: bool) -> None:
        schemas = {data_type: [field["name"] for field in self.parse_schema(data_type)]
                   for data_type in self.DATA_TYPES}

        data_file_keys = self.get_files(date, s3_bucket, prefix)

        if clean:
            self.delete_gcs_prefix(self.gcs_client.bucket(gcs_bucket),
                                   self.get_gcs_prefix(prefix, version, date))

        file_history = self.get_previously_imported_files(gcs_bucket, prefix, version, date)

        # Transform data file into csv for each data type and then save to GCS
        for i, key in enumerate(data_file_keys):
            data_file_name = os.path.basename(key)
            if data_file_name in file_history:
                logging.info(f"Skipping export for {data_file_name}")
                continue

            with tempfile.TemporaryDirectory() as data_dir:
                csv_file_paths = self.transform_data_file(key, schemas, data_dir, s3_bucket)

                for data_type, csv_file_path in csv_file_paths.items():
                    self.write_to_gcs(csv_file_path, data_type, gcs_bucket, prefix, version, date)

            self.write_to_gcs(data_file_name, self.FILE_HISTORY_PREFIX,
                              gcs_bucket, prefix, version, date)

        self.create_external_tables(gcs_bucket, prefix, date, self.DATA_TYPES,
                                    self.TMP_DATASET, dataset, table_prefix, version)
        self.delete_existing_data(dataset, table_prefix, self.DATA_TYPES, version, date)
        self.load_tables(self.TMP_DATASET, dataset, table_prefix, self.DATA_TYPES, version, date)
        self.drop_external_tables(self.TMP_DATASET, dataset, table_prefix,
                                  self.DATA_TYPES, version, date)
