import json
import os
from unittest.mock import patch, Mock, PropertyMock

import pytest
from google.cloud import bigquery

from leanplum_data_export.base_exporter import BaseLeanplumExporter


@pytest.fixture
def exporter():
    return BaseLeanplumExporter("projectId")


@pytest.fixture
def sample_data():
    with open(os.path.join(os.path.dirname(__file__), "sample.ndjson")) as f:
        return [json.loads(line) for line in f.readlines()]


class TestBaseExporter(object):

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

        with patch('leanplum_data_export.base_exporter.bigquery', spec=True) as MockBq:
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

        with patch('leanplum_data_export.base_exporter.bigquery', spec=True) as MockBq:
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

        with patch('leanplum_data_export.base_exporter.bigquery', spec=True) as MockBq:
            mock_bq_client = Mock()
            exporter.bq_client = mock_bq_client
            mock_external_config = PropertyMock()
            MockBq.SchemaField.side_effect = bigquery.SchemaField
            MockBq.ExternalConfig.return_value = mock_external_config

            with pytest.raises(Exception):
                exporter.create_external_tables(
                    bucket, prefix, date, tables, ext_dataset_name, dataset_name, table_prefix, 1)

    def test_extract_user_attributes(self, exporter, sample_data):
        user_attrs = exporter.extract_user_attributes(sample_data[0])
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
        states = exporter.extract_states(sample_data[0])
        expected = []
        assert expected == states

    def test_extract_experiments(self, exporter, sample_data):
        experiments = exporter.extract_experiments(sample_data[0])
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
        events, event_params = exporter.extract_events(sample_data[0])
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
        session = exporter.extract_session(sample_data[0], session_columns)

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
