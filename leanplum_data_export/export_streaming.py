import csv
import json
import logging
import os
import re
import tempfile
from pathlib import Path

import boto3

from .base_exporter import BaseLeanplumExporter


class StreamingLeanplumExporter(BaseLeanplumExporter):

    DATA_TYPES = [
        "eventparameters", "events", "experiments", "sessions", "states", "userattributes"
    ]

    def __init__(self, project):
        super().__init__(project)
        self.s3_client = boto3.client("s3")

    def get_files(self, date, bucket, prefix):
        """
        Get the s3 keys of the data files in the given bucket
        """
        data_file_keys = []

        continuation_token = {}  # value used for pagination
        while True:
            object_list = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=os.path.join(prefix, date, "export-"),
                **continuation_token,
            )
            data_file_keys.extend([content["Key"] for content in object_list["Contents"]])

            if not object_list["IsTruncated"]:
                break

            continuation_token["ContinuationToken"] = object_list["NextContinuationToken"]

        return data_file_keys

    def write_to_gcs(self, file_path, data_type, bucket_ref, prefix, version, date):
        """
        Write file to GCS bucket so it can be transformed and loaded into Bigquery later
        This is also to circumvent the 1500 load job limit per table per day in Bigquery
        """
        gcs_path = os.path.join(self.get_gcs_prefix(prefix, version, date),
                                data_type, file_path.name)

        logging.info(f"Uploading {file_path.name} to gs://{gcs_path}")
        blob = bucket_ref.blob(gcs_path)
        blob.upload_from_filename(str(file_path))

    def write_to_csv(self, csv_writers, session_data, schemas):
        for user_attribute in self.extract_user_attributes(session_data):
            csv_writers["userattributes"].writerow(user_attribute)

        for state in self.extract_states(session_data):
            csv_writers["sessions"].writerow(state)

        for experiment in self.extract_experiments(session_data):
            csv_writers["experiments"].writerow(experiment)

        csv_writers["sessions"].writerow(
            self.extract_session(session_data, schemas["sessions"]))

        events, event_parameters = self.extract_events(session_data)
        for event in events:
            csv_writers["events"].writerow(event)
        for event_parameter in event_parameters:
            csv_writers["eventparameters"].writerow(event_parameter)

    def transform_data_file(self, data_file_key, schemas, data_dir, bucket):
        """
        Get data file contents and convert to CSV for each data type and
        return paths to the files
        """
        logging.info(f"Exporting {data_file_key}")
        data_file = self.s3_client.get_object(
            Bucket=bucket,
            Key=data_file_key,
        )

        file_id = "-".join(data_file_key.split("-")[2:])
        csv_file_paths = {data_type: Path(os.path.join(data_dir, f"{data_type}-{file_id}.csv"))
                          for data_type in self.DATA_TYPES}
        csv_files = {data_type: open(file_path, "w")
                     for data_type, file_path in csv_file_paths.items()}
        try:
            csv_writers = {data_type: csv.DictWriter(csv_files[data_type], schemas[data_type])
                           for data_type in self.DATA_TYPES}
            for csv_writer in csv_writers.values():
                csv_writer.writeheader()

            for line in data_file["Body"].iter_lines():
                session_data = json.loads(line)
                self.write_to_csv(csv_writers, session_data, schemas)
        finally:
            for csv_file in csv_files.values():
                csv_file.close()

        return csv_file_paths

    def export(self, date, s3_bucket, gcs_bucket, prefix, dataset, table_prefix, version):
        schemas = {data_type: [field["name"] for field in self.parse_schema(data_type)]
                   for data_type in self.DATA_TYPES}

        data_file_keys = self.get_files(date, s3_bucket, prefix)

        filename_re = re.compile(r"^.*/\d{8}/export-.*-output-([0-9]+)$")

        gcs_bucket_ref = self.gcs_client.bucket(gcs_bucket)
        self.delete_gcs_prefix(gcs_bucket_ref, self.get_gcs_prefix(prefix, version, date))

        # Transform data file into csv for each data type and then save to GCS
        for key in data_file_keys:
            if filename_re.fullmatch(key) is None:  # not a data file
                continue

            with tempfile.TemporaryDirectory() as data_dir:
                csv_file_paths = self.transform_data_file(key, schemas, data_dir, s3_bucket)

                for data_type, csv_file_path in csv_file_paths.items():
                    self.write_to_gcs(csv_file_path, data_type, gcs_bucket_ref,
                                      prefix, version, date)

            break  # TODO: remove

        self.create_external_tables(gcs_bucket, prefix, date, self.DATA_TYPES,
                                    self.TMP_DATASET, dataset, table_prefix, version)
        self.delete_existing_data(dataset, table_prefix, self.DATA_TYPES, version, date)
        self.load_tables(self.TMP_DATASET, dataset, table_prefix, self.DATA_TYPES, version, date)
        self.drop_external_tables(self.TMP_DATASET, dataset, table_prefix,
                                  self.DATA_TYPES, version, date)
