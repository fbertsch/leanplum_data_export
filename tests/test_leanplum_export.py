import json
import os
import tempfile
from pathlib import Path
from unittest.mock import ANY, call, patch, Mock, PropertyMock

import boto3
import pytest
from moto import mock_s3
from google.cloud import bigquery

from leanplum_data_export import data_parser
from leanplum_data_export.export import LeanplumExporter


@pytest.fixture
def exporter():
    return LeanplumExporter("projectId")


@pytest.fixture
def sample_data():
    with open(os.path.join(os.path.dirname(__file__), "sample.ndjson")) as f:
        return [json.loads(line) for line in f.readlines()]


class TestStreamingExporter(object):

    def test_delete_gcs_prefix(self, exporter):
        client, bucket, blobs = Mock(), Mock(), Mock()
        prefix = "hello"

        type(blobs).pages = PropertyMock(return_value=[["hello/world"]])
        client.list_blobs.return_value = blobs

        exporter.gcs_client = client
        exporter.delete_gcs_prefix(bucket, prefix)

        client.list_blobs.assert_called_with(bucket, prefix=prefix)
        bucket.delete_blobs.assert_called_with(blobs.pages[0])

    def test_delete_gcs_prefix_pagination(self, exporter):
        client, bucket, blobs = Mock(), Mock(), Mock()
        prefix = "hello"

        type(blobs).pages = PropertyMock(return_value=[["hello/world"] * 1000] * 5)
        client.list_blobs.return_value = blobs

        exporter.gcs_client = client
        exporter.delete_gcs_prefix(bucket, prefix)

        assert bucket.delete_blobs.call_count == 5

    def test_created_external_tables(self, exporter):
        date = "20190101"
        bucket = 'abucket'
        prefix = 'aprefix'
        ext_dataset_name = "ext_dataset"
        dataset_name = "leanplum_dataset"
        tables = ["sessions"]
        table_prefix = "prefix"

        with patch('leanplum_data_export.export.bigquery', spec=True) as MockBq:
            mock_bq_client, mock_dataset_ref = Mock(), Mock()
            mock_table_ref, mock_table, mock_config = Mock(), Mock(), Mock()
            mock_bq_client.dataset.return_value = mock_dataset_ref
            MockBq.TableReference.return_value = mock_table_ref
            MockBq.Table.return_value = mock_table
            MockBq.ExternalConfig.return_value = mock_config

            exporter.bq_client = mock_bq_client
            exporter.create_external_tables(
                bucket, prefix, date, tables, ext_dataset_name, dataset_name, table_prefix, 1)

            mock_bq_client.dataset.assert_any_call(ext_dataset_name)
            mock_bq_client.delete_table.assert_called_with(mock_table, not_found_ok=True)
            MockBq.TableReference.assert_any_call(
                mock_dataset_ref,
                f"{dataset_name}_{table_prefix}_sessions_v1_{date}"
            )
            MockBq.Table.assert_any_call(mock_table_ref)
            MockBq.ExternalConfig.assert_any_call("CSV")

            expected_source_uris = [f"gs://{bucket}/{prefix}/v1/{date}/sessions/*"]
            assert mock_config.source_uris == expected_source_uris
            assert mock_table.external_data_configuration == mock_config
            mock_bq_client.create_table.assert_any_call(mock_table)

    def test_external_table_can_read_schema(self, exporter):
        date = "20190101"
        bucket = 'abucket'
        prefix = 'aprefix'
        ext_dataset_name = "ext_dataset"
        dataset_name = "leanplum_dataset"
        tables = ["sessions"]
        table_prefix = "prefix"

        with patch('leanplum_data_export.export.bigquery', spec=True) as MockBq:
            mock_bq_client = Mock()
            exporter.bq_client = mock_bq_client
            mock_external_config = PropertyMock()
            MockBq.SchemaField.side_effect = bigquery.SchemaField
            MockBq.ExternalConfig.return_value = mock_external_config

            exporter.create_external_tables(
                bucket, prefix, date, tables, ext_dataset_name, dataset_name, table_prefix, 1)

            assert len(mock_external_config.schema) > 0

    def test_external_table_unrecognized_table(self, exporter):
        date = "20190101"
        bucket = 'abucket'
        prefix = 'aprefix'
        ext_dataset_name = "ext_dataset"
        dataset_name = "leanplum_dataset"
        tables = ["some_unknown_table"]
        table_prefix = "prefix"

        with patch('leanplum_data_export.export.bigquery', spec=True) as MockBq:
            mock_bq_client = Mock()
            exporter.bq_client = mock_bq_client
            mock_external_config = PropertyMock()
            MockBq.SchemaField.side_effect = bigquery.SchemaField
            MockBq.ExternalConfig.return_value = mock_external_config

            with pytest.raises(Exception):
                exporter.create_external_tables(
                    bucket, prefix, date, tables, ext_dataset_name, dataset_name, table_prefix, 1)

    def test_extract_user_attributes(self, exporter, sample_data):
        user_attrs = data_parser.extract_user_attributes(sample_data[0])
        expected = [
            {
                "sessionId": 1,
                "name": "Mailto Is Default",
                "value": "True",
            },
            {
                "sessionId": 1,
                "name": "FxA account is verified",
                "value": "False",
            },
        ]
        assert expected == user_attrs

    def test_extract_states(self, exporter, sample_data):
        states = data_parser.extract_states(sample_data[0])
        expected = []
        assert expected == states

    def test_extract_experiments(self, exporter, sample_data):
        experiments = data_parser.extract_experiments(sample_data[0])
        expected = [
            {
                "sessionId": 1,
                "experimentId": 800315004,
                "variantId": 796675005,
            },
            {
                "sessionId": 1,
                "experimentId": 842715057,
                "variantId": 858195027,
            },
        ]
        assert expected == experiments

    def test_extract_events(self, exporter, sample_data):
        events, event_params = data_parser.extract_events(sample_data[0])
        expected_events = [
            {
                "sessionId": 1,
                "stateId": -2977495587907092018,
                "info": None,
                "timeUntilFirstForUser": None,
                "eventId": 8457531699855530674,
                "eventName": "E_Opened_App",
                "start": "1.591474962721E9",
                "value": 0.0,
            },
            {
                "sessionId": 1,
                "stateId": -2977495587907092018,
                "info": None,
                "timeUntilFirstForUser": 123,
                "eventId": 5682457234720643012,
                "eventName": "E_Interact_With_Search_URL_Area",
                "start": '1.591456449492E9',
                "value": 0.0,
            },
        ]
        assert expected_events == events

        expected_params = [
            {
                "eventId": 5682457234720643012,
                "name": "p1",
                "value": "value",
            },
        ]
        assert expected_params == event_params

    def test_extract_session(self, exporter, sample_data):
        session_columns = [field["name"] for field in exporter.parse_schema("sessions")]
        session = data_parser.extract_session(sample_data[0], session_columns)

        expected_session = {
            "country": "US",
            "appVersion": "18099",
            "userStart": "1.550040022647E9",
            "priorStates": 0,
            "city": "City",
            "timezone": "America/Los_Angeles",
            "sourceAd": "link",
            "lon": "-100.13395690917969",
            "locale": "en-US_US",
            "isSession": False,
            "osVersion": "13.4.1",
            "deviceId": "a",
            "duration": 0.0,
            "osName": "iOS",
            "client": "ios",
            "lat": "67.8536262512207",
            "priorSessions": 197,
            "sourceAdGroup": "sms",
            "sourceCampaign": "fxa-conf-page",
            "priorEvents": 437,
            "sessionId": "1",
            "userId": "a",
            "timezoneOffset": -25200,
            "priorTimeSpentInApp": 38830.438,
            "deviceModel": "iPhone X",
            "sourcePublisher": "Product Marketing (Owned media)",
            "sdkVersion": "2.7.2",
            "start": "1.591474962721E9",
            "region": "CA",
            "userBucket": 767,
            "isDeveloper": False,
            "browserName": None,
            "browserVersion": None,
            'sourcePublisherId': None,
            'sourceSite': None,
            'sourceSubPublisher': None,
        }

        assert expected_session == session

    def test_parse_schema(self, exporter):
        session_fields = [field["name"] for field in exporter.parse_schema("sessions")]

        expected_fields = [
            "sessionId", "userId", "userBucket", "userStart", "country", "region", "city",
            "start", "duration", "lat", "lon", "locale", "timezone", "timezoneOffset",
            "appVersion", "client", "sdkVersion", "osName", "osVersion", "deviceModel",
            "browserName", "browserVersion", "deviceId", "priorEvents", "priorSessions",
            "priorTimeSpentInApp", "priorStates", "isDeveloper", "isSession", "sourcePublisherId",
            "sourcePublisher", "sourceSubPublisher", "sourceSite", "sourceCampaign",
            "sourceAdGroup", "sourceAd",
        ]

        assert set(expected_fields) == set(session_fields)

    def test_parse_schema_invalid_schema(self, exporter):
        with pytest.raises(ValueError):
            exporter.parse_schema("unknown")

    def test_get_gcs_prefix(self, exporter):
        assert "firefox/v1/20200601/" == exporter.get_gcs_prefix("firefox", "1", "20200601")

    @mock_s3
    def test_get_files_data_file_filtering(self):
        # can't use fixture because it's instantiated before moto
        exporter = LeanplumExporter("projectId")

        bucket_name = "bucket"
        date = "20200601"
        prefix = "firefox"

        s3_client = boto3.client("s3")
        s3_client.create_bucket(Bucket=bucket_name)

        keys = [
            "firefox/20200601/1",
            "firefox/20200601/export-1-abc-output-0",
            "firefox2/20200601/export-1-abc-output-0",
            "firefox/20200601/export-2-dsaf-output-0",
            "firefox/20200602/export-1-abc-output-0",
        ]
        for key in keys:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=key,
            )

        retrieved_keys = exporter.get_files(date, bucket_name, prefix)
        expected = {
            "firefox/20200601/export-1-abc-output-0",
            "firefox/20200601/export-2-dsaf-output-0",
        }
        assert expected == set(retrieved_keys)

    @mock_s3
    def test_get_files_data_file_filtering(self):
        # can't use fixture because it's instantiated before moto3e
        exporter = LeanplumExporter("projectId")

        bucket_name = "bucket"
        date = "20200601"
        prefix = "firefox"

        s3_client = boto3.client("s3")
        s3_client.create_bucket(Bucket=bucket_name)

        with pytest.raises(AssertionError):
            exporter.get_files(date, bucket_name, prefix)

    @mock_s3
    def test_get_files_pagination(self):
        # can't use fixture because it's instantiated before moto
        streaming_exporter = LeanplumExporter("projectId")
        bucket_name = "bucket"
        date = "20200601"
        prefix = "firefox"

        s3_client = boto3.client("s3")
        s3_client.create_bucket(Bucket=bucket_name)

        keys = [
            "firefox/20200601/export-1-abc-output-0",
            "firefox/20200601/export-2-abc-output-0",
            "firefox/20200601/export-3-abc-output-0",
            "firefox/20200601/export-4-abc-output-0",
            "firefox/20200601/5",
        ]
        for key in keys:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=key,
            )
        retrieved_keys = streaming_exporter.get_files(date, bucket_name, prefix, max_keys=1)
        expected = {
            "firefox/20200601/export-1-abc-output-0",
            "firefox/20200601/export-2-abc-output-0",
            "firefox/20200601/export-3-abc-output-0",
            "firefox/20200601/export-4-abc-output-0",
        }
        # can't directly test list_objects call count
        assert expected == set(retrieved_keys)

    @patch("google.cloud.storage.Client")
    def test_write_to_gcs_upload_file(self, mock_gcs, exporter):
        mock_bucket, mock_blob = Mock(), Mock()
        mock_gcs.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        exporter.gcs_client = mock_gcs
        exporter.write_to_gcs(Path("/a/b/c"), "sessions", "bucket", "firefox", "1", "20200601")

        mock_bucket.blob.assert_called_once_with("firefox/v1/20200601/sessions/c", chunk_size=ANY)
        mock_blob.upload_from_filename.assert_called_once_with("/a/b/c")

    @patch("google.cloud.storage.Client")
    def test_write_to_gcs_given_name(self, mock_gcs, exporter):
        mock_bucket, mock_blob = Mock(), Mock()
        mock_gcs.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        exporter.gcs_client = mock_gcs
        exporter.write_to_gcs(Path("/a/b/c"), "sessions", "bucket", "firefox", "1", "20200601",
                              file_name="d")

        mock_bucket.blob.assert_called_once_with("firefox/v1/20200601/sessions/d", chunk_size=ANY)
        mock_blob.upload_from_filename.assert_called_once_with("/a/b/c")

    def test_write_to_csv_write_count(self, exporter, sample_data):
        csv_writers = {data_type: Mock() for data_type in exporter.DATA_TYPES}
        schemas = {"sessions": [field["name"] for field in exporter.parse_schema("sessions")]}
        session_data = sample_data[1]

        exporter.write_to_csv(csv_writers, session_data, schemas)

        assert csv_writers["userattributes"].writerow.call_count == 2
        assert csv_writers["states"].writerow.call_count == 0
        assert csv_writers["experiments"].writerow.call_count == 3
        assert csv_writers["sessions"].writerow.call_count == 1
        assert csv_writers["events"].writerow.call_count == 5
        assert csv_writers["eventparameters"].writerow.call_count == 4

    @mock_s3
    def test_transform_data_file_data_read(self):
        # can't use fixture because it's instantiated before moto
        exporter = LeanplumExporter("projectId")

        bucket_name = "bucket"
        data_file_key = "data_file"
        schemas = {data_type: ["field"] for data_type in exporter.DATA_TYPES}

        s3_client = boto3.client("s3")
        s3_client.create_bucket(Bucket=bucket_name)
        s3_client.upload_file(os.path.join(os.path.dirname(__file__), "sample.ndjson"),
                              bucket_name, data_file_key)

        exporter.write_to_csv = Mock()

        with tempfile.TemporaryDirectory() as data_dir:
            exporter.transform_data_file(data_file_key, schemas, data_dir, bucket_name)

        assert exporter.write_to_csv.call_count == 2

    def test_export_file_count(self, exporter):
        exporter.get_files = Mock()
        exporter.get_previously_imported_files = Mock()
        exporter.delete_gcs_prefix = Mock()
        exporter.create_external_tables = Mock()
        exporter.delete_existing_data = Mock()
        exporter.load_tables = Mock()
        exporter.drop_external_tables = Mock()
        exporter.transform_data_file = Mock()
        exporter.write_to_gcs = Mock()

        exporter.transform_data_file.return_value = {
            "a": "1",
            "b": "2",
        }
        exporter.get_previously_imported_files.return_value = set()

        files = [str(i) for i in range(1, 1000)]

        exporter.get_files.return_value = files
        exporter.export("20200601", "s3", "gcs", "prefix", "dataset",
                        "table_prefix", "version", True)

        exporter.transform_data_file.assert_has_calls(
            [call(i, ANY, ANY, ANY) for i in files],
            any_order=True
        )
        assert exporter.transform_data_file.call_count == 999
        assert exporter.write_to_gcs.call_count == 2997

        # assert all clean up and creation steps are done
        exporter.delete_gcs_prefix.assert_called_once()
        exporter.create_external_tables.assert_called_once()
        exporter.delete_existing_data.assert_called_once()
        exporter.load_tables.assert_called_once()
        exporter.drop_external_tables.assert_called_once()

    def test_export_previously_written_files(self, exporter):
        exporter.get_files = Mock()
        exporter.get_files.return_value = [
            "a/b/file1",
            "a/b/file2",
            "a/b/file3",
            "a/b/file4",
        ]
        exporter.get_previously_imported_files = Mock()
        exporter.get_previously_imported_files.return_value = [
            "c/d/file1",
            "file3",
        ]
        exporter.transform_data_file = Mock()
        exporter.transform_data_file.return_value = {}
        exporter.write_to_gcs = Mock()
        exporter.delete_gcs_prefix = Mock()
        exporter.create_external_tables = Mock()
        exporter.delete_existing_data = Mock()
        exporter.load_tables = Mock()
        exporter.drop_external_tables = Mock()

        exporter.export("20200601", "s3", "gcs", "prefix", "dataset",
                        "table_prefix", "version", False)

        exporter.transform_data_file.assert_has_calls([
            call("a/b/file2", ANY, ANY, ANY),
            call("a/b/file4", ANY, ANY, ANY),
        ])
        exporter.write_to_gcs.assert_has_calls([
            call(ANY, ANY, ANY, ANY, ANY, ANY, file_name="file2"),
            call(ANY, ANY, ANY, ANY, ANY, ANY, file_name="file4"),
        ])
        exporter.delete_gcs_prefix.assert_not_called()
