import pytest
import responses
import requests
import os
import re

from unittest.mock import patch, Mock
from leanplum_data_export.export import LeanplumExporter
from google.cloud import exceptions


app_id = "appid"
client_key = "clientkey"


@pytest.fixture
def exporter():
    return LeanplumExporter(app_id, client_key)


class BlobList:
    def __init__(self, blobs):
        self.pages = [blobs]


class TestExporter(object):

    def test_add_slash(self, exporter):
        assert exporter.add_slash_if_not_present("hello") == "hello/"

    def test_dont_add_slash(self, exporter):
        assert exporter.add_slash_if_not_present("hello/") == "hello/"

    @responses.activate
    def test_init_export(self, exporter):
        job_id = "testjobid"
        expected_url = (f"http://www.leanplum.com/api?appId={app_id}&clientKey={client_key}"
                        f"&apiVersion=1.0.6&action=exportData&startDate=20190101&exportFormat=json")
        responses.add(
            responses.GET,
            expected_url,
            json={"response": [{"jobId": job_id}]},
            status=200)

        res_job_id = exporter.init_export("20190101", "json")
        assert res_job_id == job_id

    @responses.activate
    def test_init_export_error(self, exporter):
        job_id = "testjobid"
        responses.add(
            responses.GET,
            'http://www.leanplum.com/api',
            json={"response": [{"jobId": job_id}]},
            status=404)

        with pytest.raises(requests.exceptions.HTTPError):
            exporter.init_export("20190101", "json")

    @responses.activate
    def test_get_files(self, exporter):
        job_id = "testjobid"
        responses.add(
            responses.GET,
            'http://www.leanplum.com/api',
            json={"response": [{"state": "not finished"}]},
            status=200)

        responses.add(
            responses.GET,
            'http://www.leanplum.com/api',
            json={"response": [{"state": "FINISHED", "files": ["fileuri"]}]},
            status=200)

        file_uris = exporter.get_files(job_id, sleep_time=0)
        assert file_uris == ["fileuri"]

    @responses.activate
    def test_save_files(self, exporter):
        file_uris = [('https://leanplum_export.storage.googleapis.com/export-'
                      '5094741967896576-60c43e66-30fe-4e21-9bbd-563d2749b96f-outputsessions-0')]
        bucket = 'abucket'
        prefix = 'aprefix'
        file_body = b"data"
        date = "20190101"

        responses.add(
             responses.GET,
             re.compile(r"https://leanplum_export.storage.googleapis.com.*"),
             body=file_body,
             status=200)

        self.file_contents = None

        def set_contents(filename):
            with open(filename, "rb") as f:
                self.file_contents = f.read()

        mock_bucket, mock_client, mock_blob = Mock(), Mock(), Mock()

        mock_client.list_blobs.return_value = BlobList([])
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.upload_from_filename.side_effect = set_contents

        exporter.gcs_client = mock_client
        exporter.save_files(file_uris, bucket, prefix, date, "json", 1)

        suffix = f"sessions/0.json"
        mock_client.bucket.assert_called_with(bucket)
        mock_bucket.blob.assert_called_with(f"{prefix}/v1/{date}/{suffix}")
        mock_blob.upload_from_filename.assert_called_with(suffix)
        assert self.file_contents == file_body
        assert not os.path.isfile(suffix)
        assert not os.path.isdir("sessions")

    @responses.activate
    def test_save_files_no_prefix(self, exporter):
        file_uris = [('https://leanplum_export.storage.googleapis.com/export'
                     '-5094741967896576-60c43e66-30fe-4e21-9bbd-563d2749b96f-outputsessions-0')]
        bucket = 'abucket'
        prefix = ''
        file_body = b"data"
        date = "20190101"

        responses.add(
             responses.GET,
             re.compile(r"https://leanplum_export.storage.googleapis.com.*"),
             body=file_body,
             status=200)

        self.file_contents = None

        def set_contents(filename):
            with open(filename, "rb") as f:
                self.file_contents = f.read()

        mock_bucket, mock_client, mock_blob = Mock(), Mock(), Mock()

        mock_client.list_blobs.return_value = BlobList([])
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.upload_from_filename.side_effect = set_contents

        exporter.gcs_client = mock_client
        exporter.save_files(file_uris, bucket, prefix, date, "json", 1)

        suffix = f"sessions/0.json"
        mock_client.bucket.assert_called_with(bucket)
        mock_bucket.blob.assert_called_with(f"v1/{date}/{suffix}")
        mock_blob.upload_from_filename.assert_called_with(suffix)
        assert self.file_contents == file_body
        assert not os.path.isfile(suffix)
        assert not os.path.isdir("sessions")

    @responses.activate
    def test_save_files_multiple_uris(self, exporter):
        n_files = 5
        base_uri = ("https://leanplum_export.storage.googleapis.com/export"
                    "-5094741967896576-60c43e66-30fe-4e21-9bbd-563d2749b96f-output")
        file_types = ["sessions", "experiments"]
        file_uris = [f'{base_uri}{ftype}-{i}' for ftype in file_types for i in range(n_files)]
        bucket = 'abucket'
        prefix = ''
        file_body = b"data"
        date = "20190101"

        responses.add(
             responses.GET,
             re.compile(r"https://leanplum_export.storage.googleapis.com.*"),
             body=file_body,
             status=200)

        self.file_contents = None

        def set_contents(filename):
            with open(filename, "rb") as f:
                self.file_contents = f.read()

        mock_bucket, mock_client, mock_blob = Mock(), Mock(), Mock()

        mock_client.list_blobs.return_value = BlobList(["hello/world"])
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.upload_from_filename.side_effect = set_contents

        exporter.gcs_client = mock_client
        tables = exporter.save_files(file_uris, bucket, prefix, date, "json", 1)
        mock_client.bucket.assert_called_with(bucket)
        assert tables == set(file_types)

        for ftype in file_types:
            for i in range(n_files):
                suffix = f"{ftype}/{i}.json"
                mock_bucket.blob.assert_any_call(f"v1/{date}/{suffix}")
                mock_blob.upload_from_filename.assert_any_call(suffix)
                assert self.file_contents == file_body
                assert not os.path.isfile(suffix)
                assert not os.path.isdir(ftype)

    @responses.activate
    def test_save_files_improper_file_format(self, exporter):
        file_uris = [('https://leanplum_export.storage.googleapis.com/export'
                      '-5094741967896576-60c43e66-30fe-4e21-9bbd-563d2749b96f-outputsessions')]
        bucket = 'abucket'
        prefix = ''
        file_body = b"data"
        date = "20190101"

        responses.add(
             responses.GET,
             re.compile(r"https://leanplum_export.storage.googleapis.com.*"),
             body=file_body,
             status=200)

        with pytest.raises(Exception):
            exporter.save_files(file_uris, bucket, prefix, date, "json", 1)

    @responses.activate
    def test_export_new_table(self, exporter):
        date = "20190101"
        job_id = "testjobid"
        file_uris = [('https://leanplum_export.storage.googleapis.com/export'
                      '-5094741967896576-60c43e66-30fe-4e21-9bbd-563d2749b96f-outputsessions-0')]
        bucket = 'abucket'
        prefix = 'aprefix'
        file_body = b"data"
        tmp_dataset = "tmp"
        dataset_name = "leanplum_dataset"

        responses.add(
            responses.GET,
            'http://www.leanplum.com/api',
            json={"response": [{"jobId": job_id}]},
            status=200)
        responses.add(
            responses.GET,
            'http://www.leanplum.com/api',
            json={"response": [{"state": "FINISHED", "files": file_uris}]},
            status=200)
        responses.add(
            responses.GET,
            re.compile(r"https://leanplum_export.storage.googleapis.com.*"),
            body=file_body,
            status=200)

        self.file_contents = None

        def set_contents(filename):
            with open(filename, "rb") as f:
                self.file_contents = f.read()

        with patch('leanplum_data_export.export.bigquery', spec=True) as MockBq:
            with patch('leanplum_data_export.export.storage', spec=True) as MockStorage:
                mock_bucket, mock_client, mock_blob = Mock(), Mock(), Mock()

                mock_client.list_blobs.return_value = BlobList([])
                mock_client.bucket.return_value = mock_bucket
                mock_bucket.blob.return_value = mock_blob
                mock_blob.upload_from_filename.side_effect = set_contents
                MockStorage.Client.return_value = mock_client

                mock_bq_client, mock_dataset_ref = Mock(), Mock()
                mock_table_ref, mock_table, mock_config = Mock(), Mock(), Mock()
                mock_bq_client.dataset.return_value = mock_dataset_ref
                mock_bq_client.get_table.side_effect = exceptions.NotFound('')
                MockBq.Client.return_value = mock_bq_client
                MockBq.TableReference.return_value = mock_table_ref
                MockBq.Table.return_value = mock_table
                MockBq.ExternalConfig.return_value = mock_config

                exporter.export(date, bucket, prefix, dataset_name, "", 1, "test-project")

                suffix = f"sessions/0.csv"
                mock_client.bucket.assert_called_with(bucket)
                mock_bucket.blob.assert_called_with(f"{prefix}/v1/{date}/{suffix}")
                mock_blob.upload_from_filename.assert_called_with(suffix)
                assert self.file_contents == file_body
                assert not os.path.isfile(suffix)
                assert not os.path.isdir("sessions")

                mock_bq_client.dataset.assert_any_call(tmp_dataset)
                mock_bq_client.delete_table.assert_any_call(mock_table, not_found_ok=True)
                MockBq.TableReference.assert_any_call(mock_dataset_ref, f"sessions_v1_{date}")
                MockBq.Table.assert_any_call(mock_table_ref)
                MockBq.ExternalConfig.assert_any_call("CSV")

                expected_source_uris = [f"gs://{bucket}/{prefix}/v1/{date}/sessions/*"]
                assert mock_config.source_uris == expected_source_uris
                assert mock_config.autodetect is True
                assert mock_table.external_data_configuration == mock_config
                mock_bq_client.create_table.assert_any_call(mock_table)

                mock_bq_client.dataset.assert_any_call(dataset_name)
                MockBq.TableReference.assert_any_call(mock_dataset_ref, "sessions_v1")

                delete_query = (
                    f"DELETE FROM `{dataset_name}.sessions_v1` "
                    f"WHERE load_date = PARSE_DATE('%Y%m%d', '{date}')")
                mock_bq_client.query.assert_any_call(delete_query)

                expected_query = (
                    f"CREATE TABLE `{dataset_name}.sessions_v1` "
                    f"PARTITION BY load_date AS SELECT * EXCEPT (lat,lon), "
                    f"PARSE_DATE('%Y%m%d', '{date}') AS load_date FROM `tmp.sessions_v1_{date}`")
                mock_bq_client.query.assert_any_call(expected_query)

                mock_bq_client.delete_table.assert_any_call(mock_table)

    @responses.activate
    def test_export_existing_table(self, exporter):
        date = "20190101"
        job_id = "testjobid"
        file_uris = [('https://leanplum_export.storage.googleapis.com/export'
                      '-5094741967896576-60c43e66-30fe-4e21-9bbd-563d2749b96f-outputsessions-0')]
        bucket = 'abucket'
        prefix = 'aprefix'
        file_body = b"data"
        tmp_dataset = "tmp"
        dataset_name = "leanplum_dataset"

        responses.add(
            responses.GET,
            'http://www.leanplum.com/api',
            json={"response": [{"jobId": job_id}]},
            status=200)
        responses.add(
            responses.GET,
            'http://www.leanplum.com/api',
            json={"response": [{"state": "FINISHED", "files": file_uris}]},
            status=200)
        responses.add(
            responses.GET,
            re.compile(r"https://leanplum_export.storage.googleapis.com.*"),
            body=file_body,
            status=200)

        self.file_contents = None

        def set_contents(filename):
            with open(filename, "rb") as f:
                self.file_contents = f.read()

        with patch('leanplum_data_export.export.bigquery', spec=True) as MockBq:
            with patch('leanplum_data_export.export.storage', spec=True) as MockStorage:
                mock_bucket, mock_client, mock_blob = Mock(), Mock(), Mock()

                mock_client.list_blobs.return_value = BlobList([])
                mock_client.bucket.return_value = mock_bucket
                mock_bucket.blob.return_value = mock_blob
                mock_blob.upload_from_filename.side_effect = set_contents
                MockStorage.Client.return_value = mock_client

                mock_bq_client, mock_dataset_ref = Mock(), Mock()
                mock_table_ref, mock_table, mock_config = Mock(), Mock(), Mock()
                mock_bq_client.dataset.return_value = mock_dataset_ref
                MockBq.Client.return_value = mock_bq_client
                MockBq.TableReference.return_value = mock_table_ref
                MockBq.Table.return_value = mock_table
                MockBq.ExternalConfig.return_value = mock_config

                exporter.export(date, bucket, prefix, dataset_name, "", 1, "test-project")

                suffix = f"sessions/0.csv"
                mock_client.bucket.assert_called_with(bucket)
                mock_bucket.blob.assert_called_with(f"{prefix}/v1/{date}/{suffix}")
                mock_blob.upload_from_filename.assert_called_with(suffix)
                assert self.file_contents == file_body
                assert not os.path.isfile(suffix)
                assert not os.path.isdir("sessions")

                mock_bq_client.dataset.assert_any_call(tmp_dataset)
                mock_bq_client.delete_table.assert_any_call(mock_table, not_found_ok=True)
                MockBq.TableReference.assert_any_call(mock_dataset_ref, f"sessions_v1_{date}")
                MockBq.Table.assert_any_call(mock_table_ref)
                MockBq.ExternalConfig.assert_any_call("CSV")

                expected_source_uris = [f"gs://{bucket}/{prefix}/v1/{date}/sessions/*"]
                assert mock_config.source_uris == expected_source_uris
                assert mock_config.autodetect is True
                assert mock_table.external_data_configuration == mock_config
                mock_bq_client.create_table.assert_any_call(mock_table)

                mock_bq_client.dataset.assert_any_call(dataset_name)
                MockBq.TableReference.assert_any_call(mock_dataset_ref, "sessions_v1")

                delete_query = (
                    f"DELETE FROM `{dataset_name}.sessions_v1` "
                    f"WHERE load_date = PARSE_DATE('%Y%m%d', '{date}')")
                mock_bq_client.query.assert_any_call(delete_query)

                insert_query = (
                    f"INSERT INTO `{dataset_name}.sessions_v1` SELECT * EXCEPT (lat,lon), "
                    f"PARSE_DATE('%Y%m%d', '{date}') AS load_date FROM `tmp.sessions_v1_{date}`")
                mock_bq_client.query.assert_any_call(insert_query)

                mock_bq_client.delete_table.assert_any_call(mock_table)

    def test_delete_gcs_prefix(self, exporter):
        client, bucket = Mock(), Mock()
        blobs = ["hello/world"]
        prefix = "hello"

        client.list_blobs.return_value = BlobList(blobs)

        exporter.gcs_client = client
        exporter.delete_gcs_prefix(bucket, prefix)

        client.list_blobs.assert_called_with(bucket, prefix=prefix)
        bucket.delete_blobs.assert_called_with(blobs)

    def test_delete_gcs_prefix_pagination(self, exporter):
        client, bucket = Mock(), Mock()
        prefix = "hello"

        class BlobList:
            pages = [["hello/world"] * 1000] * 5

        client.list_blobs.return_value = BlobList()

        exporter.gcs_client = client
        exporter.delete_gcs_prefix(bucket, prefix)

        assert bucket.delete_blobs.call_count == 5

    def test_created_external_tables(self, exporter):
        date = "20190101"
        bucket = 'abucket'
        prefix = 'aprefix'
        dataset_name = "leanplum_dataset"
        tables = ["sessions"]
        table_prefix = "prefix"

        with patch('leanplum_data_export.export.bigquery', spec=True) as MockBq:
            mock_bq_client, mock_dataset_ref = Mock(), Mock()
            mock_table_ref, mock_table, mock_config = Mock(), Mock(), Mock()
            mock_bq_client.dataset.return_value = mock_dataset_ref
            MockBq.Client.return_value = mock_bq_client
            MockBq.TableReference.return_value = mock_table_ref
            MockBq.Table.return_value = mock_table
            MockBq.ExternalConfig.return_value = mock_config

            exporter.bq_client = mock_bq_client
            exporter.create_external_tables(
                bucket, prefix, date, tables, dataset_name, table_prefix, 1)

            mock_bq_client.dataset.assert_any_call(dataset_name)
            mock_bq_client.delete_table.assert_called_with(mock_table, not_found_ok=True)
            MockBq.TableReference.assert_any_call(mock_dataset_ref,
                                                  f"{table_prefix}_sessions_v1_{date}")
            MockBq.Table.assert_any_call(mock_table_ref)
            MockBq.ExternalConfig.assert_any_call("CSV")

            expected_source_uris = [f"gs://{bucket}/{prefix}/v1/{date}/sessions/*"]
            assert mock_config.source_uris == expected_source_uris
            assert mock_config.autodetect is True
            assert mock_table.external_data_configuration == mock_config
            mock_bq_client.create_table.assert_any_call(mock_table)
