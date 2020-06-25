import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch, Mock, PropertyMock

import boto3
import pytest
from moto import mock_s3

from leanplum_data_export.export_streaming import StreamingLeanplumExporter


@pytest.fixture
def exporter():
    return StreamingLeanplumExporter("projectId")

@pytest.fixture
def sample_data():
    with open(os.path.join(os.path.dirname(__file__), "sample.ndjson")) as f:
        return [json.loads(line) for line in f.readlines()]

class TestStreamingExporter(object):

    @mock_s3
    def test_get_files_data_file_filtering(self, exporter):
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
    def test_get_files_pagination(self):
        streaming_exporter = StreamingLeanplumExporter("projectId")
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
    def test_write_to_gcs_correct_path(self, mock_gcs, exporter):
        mock_bucket, mock_blob = Mock(), Mock()
        mock_gcs.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        exporter.gcs_client = mock_gcs
        exporter.write_to_gcs(Path("/a/b/c"), "sessions", "bucket", "firefox", "1", "20200601")

        mock_bucket.blob.assert_called_once_with("firefox/v1/20200601/sessions/c")
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
    def test_transform_data_file_data_read(self, exporter):
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
        exporter.delete_gcs_prefix = Mock()
        exporter.create_external_tables = Mock()
        exporter.delete_existing_data = Mock()
        exporter.load_tables = Mock()
        exporter.drop_external_tables = Mock()
        exporter.transform_data_file = Mock()
        exporter.write_to_gcs = Mock()

        exporter.transform_data_file.return_value = {
            "a": 1,
            "b": 2,
        }
        exporter.get_files.return_value = list(range(1, 1000))

        exporter.export("20200601", "s3", "gcs", "prefix", "dataset", "table_prefix", "version")

        assert exporter.transform_data_file.call_count == 999
        assert exporter.write_to_gcs.call_count == 1998

        # assert all clean up and creation steps are done
        exporter.delete_gcs_prefix.assert_called_once()
        exporter.create_external_tables.assert_called_once()
        exporter.delete_existing_data.assert_called_once()
        exporter.load_tables.assert_called_once()
        exporter.drop_external_tables.assert_called_once()


