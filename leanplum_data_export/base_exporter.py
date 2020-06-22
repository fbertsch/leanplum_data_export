import json
import logging
import os

from google.cloud import bigquery, exceptions, storage


class BaseLeanplumExporter(object):
    TMP_DATASET = "tmp"
    DROP_COLS = {"sessions": {"lat", "lon"}}
    SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "schemas/")
    PARTITION_FIELD = "load_date"

    def __init__(self, project):
        self.bq_client = bigquery.Client(project=project)
        self.gcs_client = storage.Client(project=project)

    @staticmethod
    def extract_user_attributes(session_data):
        attributes = []
        for attribute, value in session_data.get("userAttributes", {}).items():
            attributes.append({
                "sessionId": int(session_data["sessionId"]),
                "name": attribute,
                "value": value,
            })
        return attributes

    @staticmethod
    def extract_states(session_data):
        """
        We don't seem to use states; csv export returns empty states csv's
        stateId in the exported json is a random number assigned to an event according to
        https://docs.leanplum.com/docs/reading-and-understanding-exported-sessions-data
        """
        return []

    @staticmethod
    def extract_experiments(session_data):
        experiments = []
        for experiment in session_data.get("experiments", []):
            experiments.append({
                "sessionId": int(session_data["sessionId"]),
                "experimentId": experiment["id"],
                "variantId": experiment["variantId"],
            })
        return experiments

    @staticmethod
    def extract_events(session_data):
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

    @staticmethod
    def extract_session(session_data, session_columns):
        session = {}
        for name in session_columns:
            session[name] = session_data.get(name)
        session["timezoneOffset"] = session_data.get("timezoneOffsetSeconds")
        session["osName"] = session_data.get("systemName")
        session["osVersion"] = session_data.get("systemVersion")
        session["userStart"] = session_data.get("firstRun")
        session["start"] = session_data.get("time")
        session["isDeveloper"] = session_data.get("isDeveloper", False)

        return session

    @classmethod
    def parse_schema(cls, data_type):
        try:
            drop_cols = cls.DROP_COLS.get(data_type, {})
            with open(os.path.join(cls.SCHEMA_DIR, f"{data_type}.schema.json"), "r") as schema_file:
                return [field for field in json.load(schema_file)
                        if field["name"] not in drop_cols]
        except FileNotFoundError:
            raise ValueError(f"Unrecognized table name encountered: {data_type}")

    def delete_gcs_prefix(self, bucket, prefix):
        blobs = self.gcs_client.list_blobs(bucket, prefix=prefix)

        for page in blobs.pages:
            bucket.delete_blobs(list(page))

    def create_external_tables(self, bucket_name, prefix, date, tables,
                               ext_dataset, dataset, table_prefix, version):
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
            external_config.source_uris = [f"{gcs_loc}/{leanplum_name}/*"]
            external_config.schema = schema
            external_config.options.skip_leading_rows = 1
            external_config.options.allow_quoted_newlines = True

            table.external_data_configuration = external_config

            self.bq_client.create_table(table)

    def delete_existing_data(self, dataset, table_prefix, tables, version, date):
        for table in tables:
            table_name = self.get_table_name(table_prefix, table, version)

            delete_sql = (
                f"DELETE FROM `{dataset}.{table_name}` "
                f"WHERE {self.PARTITION_FIELD} = PARSE_DATE('%Y%m%d', '{date}')")

            logging.info(f"Deleting data from {dataset}.{table_name}")
            logging.info(delete_sql)
            self.bq_client.query(delete_sql)

    def load_tables(self, ext_dataset, dataset, table_prefix, tables, version, date):
        destination_dataset = self.bq_client.dataset(dataset)

        for table in tables:
            ext_table_name = self.get_table_name(table_prefix, table, version, date, dataset)
            table_name = self.get_table_name(table_prefix, table, version)

            destination_table = bigquery.TableReference(destination_dataset, table_name)

            select_sql = (
                f"SELECT *, PARSE_DATE('%Y%m%d', '{date}') AS {self.PARTITION_FIELD} "
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

    @classmethod
    def get_gcs_prefix(cls, prefix, version, date):
        return os.path.join(prefix, f"v{version}", date)
