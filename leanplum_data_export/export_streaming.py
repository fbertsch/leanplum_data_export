import json
import logging
import os
import time

import boto3

from .base_exporter import BaseLeanplumExporter


class StreamingLeanplumExporter(BaseLeanplumExporter):

    def __init__(self):
        self.s3_client = boto3.client("s3")

    @classmethod
    def extract_user_attributes(cls, session_data):
        attributes = []
        for attribute, value in session_data.get("userAttributes", {}).items():
            attributes.append({
                "sessionId": int(session_data["sessionId"]),
                "name": attribute,
                "value": value,
            })
        return attributes

    @classmethod
    def extract_states(cls, session_data):
        """
        We don't seem to use states; csv export returns empty states csv's
        stateId in the exported json is a random number assigned to an event according to
        https://docs.leanplum.com/docs/reading-and-understanding-exported-sessions-data
        """
        return []

    @classmethod
    def extract_experiments(cls, session_data):
        experiments = []
        for experiment in session_data.get("experiments", []):
            experiments.append({
                "sessionId": int(session_data["sessionId"]),
                "experimentId": experiment["id"],
                "variantId": experiment["variantId"],
            })
        return experiments

    @classmethod
    def extract_events(cls, session_data):
        events = []
        event_parameters = []
        for state in session_data.get("states", []):
            for event in state.get("events", []):
                events.append({
                    "sessionId": int(session_data["sessionId"]),
                    "stateId": state["stateId"],
                    "eventId": event["eventId"],
                    "eventName": event["name"],
                    "start": event["time"],
                    "value": event["value"],
                    "info": event.get("info"),
                    "timeUntilFirstForUser": event.get("timeUntilFirstForUser"),
                })
                for parameter, value in event.get("parameters", {}).items():
                    event_parameters.append({
                        "eventId": event["eventId"],
                        "name": parameter,
                        "value": value,
                    })

        return events, event_parameters

    @classmethod
    def load_session_columns(cls):  # generalize to load any schema
        schema_dir = os.path.join(os.path.dirname(__file__), "schemas")
        with open(os.path.join(schema_dir, "sessions.schema.json")) as f:
            return [attribute["name"] for attribute in json.load(f)]

    @classmethod
    def extract_session(cls, session_data, session_columns):
        excluded_columns = {"lat", "lon"}
        session = {}
        for name in session_columns:
            if name not in excluded_columns:
                session[name] = session_data.get(name)
        return session

    def get_data_file_keys(self, date, bucket, prefix):
        """
        Get the s3 keys of the data files in the given bucket

        :param date:
        :param bucket:
        :param prefix:
        :return:
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

    def write_to_bq(self, csv_file_path, dataset, table_prefix, table_name):
        """
        Load data in the given CSV into a bigquery table

        :param csv_file_path:
        :param dataset:
        :param table_prefix:
        :param table_name:
        """
        pass

    def transform_data_file(self, data_file_key, session_columns, bucket,
                            dataset, table_prefix):
        """
        Get data file contents and convert to CSV to be loaded into bigquery

        :param data_file_key:
        :param session_columns:
        :param bucket:
        :param dataset:
        :param table_prefix:
        """
        logging.info(f"Exporting {data_file_key}")
        data_file = self.s3_client.get_object(
            Bucket=bucket,
            Key=data_file_key,
        )

        for line in data_file["Body"].iter_lines():
            line.decode('utf-8')

            session_data = json.loads(line)

            user_attributes = self.extract_user_attributes(session_data)
            states = self.extract_states(session_data)
            experiments = self.extract_experiments(session_data)
            events, event_parameters = self.extract_events(session_data)
            session = self.extract_session(session_data, session_columns)

            pass

    def export(self, date, bucket, prefix, dataset, table_prefix, version, project):
        session_columns = self.load_session_columns()

        data_file_keys = self.get_data_file_keys(date, bucket, prefix)

        for key in data_file_keys:
            self.transform_data_file(key, session_columns, bucket, dataset, table_prefix)
