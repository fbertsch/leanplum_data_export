import json
import logging
import os
import time

import boto3

from .base_exporter import BaseLeanplumExporter


class StreamingLeanplumExporter(BaseLeanplumExporter):

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
    def load_session_columns(cls):
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

    def export(self, date, bucket, prefix, dataset, table_prefix, version, project):
        s3_client = boto3.client("s3")

        session_columns = self.load_session_columns()

        data_file_keys = []

        continuation_token = {}  # value used for pagination
        while True:
            object_list = s3_client.list_objects_v2(  # TODO: use params
                Bucket=bucket,
                Prefix=os.path.join(prefix, date),
                **continuation_token,
            )
            data_file_keys.extend([content["Key"] for content in object_list["Contents"]])

            if not object_list["IsTruncated"]:
                break

            continuation_token["ContinuationToken"] = object_list["NextContinuationToken"]

        for key in data_file_keys:
            logging.info(f"Exporting {key}")
            logging.info(f"start: {time.time()}")
            data_file = s3_client.get_object(
                Bucket=bucket,
                Key=key,
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

            logging.info(f"end: {time.time()}")
        logging.info("a")
