"""Get all messages from leanplum API and save to bigquery."""

import datetime
import json

import requests
from google.cloud import bigquery

LEANPLUM_API_URL = "https://api.leanplum.com/api"
# latest version can be found at https://docs.leanplum.com/reference#get_api-action-getmessages
LEANPLUM_API_VERSION = "1.0.6"


class LeanplumMessageFetcher(object):
    def __init__(self, app_id, client_key, project, bq_dataset, table_prefix, version):
        self.app_id = app_id
        self.client_key = client_key
        self.project = project
        self.bq_dataset = bq_dataset
        self.table_prefix = table_prefix
        self.version = version

    def write_to_bq(self, date, messages):
        bq_client = bigquery.Client(project=self.project)

        table_name = f"messages_v{self.version}"
        if self.table_prefix is not None:
            table_name = f"{self.table_prefix}_{table_name}"

        load_config = bigquery.LoadJobConfig(
            time_partitioning=bigquery.TimePartitioning(
                field="load_date",
                require_partition_filter=True,
            ),
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema_update_options=bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        )
        load_job = bq_client.load_table_from_json(
            messages,
            destination=f"{self.bq_dataset}.{table_name}${date.replace('-', '')}",
            job_config=load_config,
        )

        load_job.result()

    def get_messages(self, date):
        messages_response = requests.get(
            LEANPLUM_API_URL,
            params=dict(
                action="getMessages",
                appId=self.app_id,
                clientKey=self.client_key,
                apiVersion=LEANPLUM_API_VERSION,
                recent=False,
            ),
        )

        messages_response.raise_for_status()

        # Get messages as dict, converting timestamps to ISO datetime strings
        # See https://docs.leanplum.com/reference#get_api-action-getmessages for response structure
        messages_json = json.loads(
            messages_response.text,
            parse_float=lambda t: datetime.datetime.fromtimestamp(float(t)).isoformat()
        )["response"][0]

        if "error" in messages_json:
            raise RuntimeError(messages_json["error"]["message"])

        messages = [
            {
                "load_date": date,
                **message,
            } for message in messages_json["messages"]
        ]

        print(f"Retrieved {len(messages)} messages")

        self.write_to_bq(date, messages)
