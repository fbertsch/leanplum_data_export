#!/usr/bin/env python3

"""Get all messages from leanplum API and save to bigquery."""

from collections import OrderedDict

import click
import requests
from google.cloud import bigquery

LEANPLUM_API_URL = "https://api.leanplum.com/api"
# latest version can be found at https://docs.leanplum.com/reference#get_api-action-getmessages
LEANPLUM_API_VERSION = "1.0.6"


@click.command()
@click.option("--date", required=True)
@click.option("--app-id", required=True)
@click.option("--client-key", required=True)
@click.option("--project", required=True)
@click.option("--bq-dataset", required=True)
@click.option("--table-prefix", default=None)
@click.option("--version", default=1)
def get_messages(date, app_id, client_key, project, bq_dataset, table_prefix, version):
    messages_response = requests.get(
        LEANPLUM_API_URL,
        params=dict(
            action="getMessages",
            appId=app_id,
            clientKey=client_key,
            apiVersion=LEANPLUM_API_VERSION,
            recent=False,
        ),
    )

    messages_response.raise_for_status()

    # See https://docs.leanplum.com/reference#get_api-action-getmessages for response structure
    messages_json = messages_response.json()["response"][0]

    if "error" in messages_json:
        raise RuntimeError(messages_json["error"]["message"])

    messages = [
        OrderedDict(
            load_date="2020-08-10",
            **message,
        ) for message in messages_json["messages"]
    ]

    print(f"Retrieved {len(messages)} messages")

    bq_client = bigquery.Client(project=project)

    table_name = f"messages_v{version}"
    if table_prefix is not None:
        table_name = f"{table_prefix}_{table_name}"

    load_config = bigquery.LoadJobConfig(
        time_partitioning=bigquery.TimePartitioning(field="load_date"),
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema_update_options=bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
    )
    load_job = bq_client.load_table_from_json(
        messages,
        destination=f"{bq_dataset}.{table_name}${date.replace('-', '')}",
        job_config=load_config,
    )

    load_job.result()


if __name__ == "__main__":
    get_messages()
