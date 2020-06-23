import logging
import os
import re
import requests
import time

from pathlib import Path

from .base_exporter import BaseLeanplumExporter


class LeanplumExporter(BaseLeanplumExporter):

    FINISHED_STATE = "FINISHED"
    DEFAULT_SLEEP_SECONDS = 10
    DEFAULT_EXPORT_FORMAT = "csv"
    FILENAME_RE = (r"^https://leanplum_export.storage.googleapis.com"
                   "/export-.*-output([a-z0-9]+)-([0-9]+)$")

    def __init__(self, project, app_id, client_key):
        super().__init__(project)

        self.app_id = app_id
        self.bq_client_key = client_key
        self.filename_re = re.compile(LeanplumExporter.FILENAME_RE)

    def export(self, date, bucket, prefix, dataset, table_prefix,
               version, export_format=DEFAULT_EXPORT_FORMAT):
        job_id = self.init_export(date, export_format)
        file_uris = self.get_files(job_id)
        tables = self.save_files(file_uris, bucket, prefix, date, export_format, version)
        self.create_external_tables(bucket, prefix, date, tables, self.TMP_DATASET,
                                    dataset, table_prefix, version)
        self.delete_existing_data(dataset, table_prefix, tables, version, date)
        self.load_tables(self.TMP_DATASET, dataset, table_prefix, tables, version, date)
        self.drop_external_tables(self.TMP_DATASET, dataset, table_prefix, tables, version, date)

    def init_export(self, date, export_format):
        export_init_url = (f"http://www.leanplum.com/api"
                           f"?appId={self.app_id}"
                           f"&clientKey={self.bq_client_key}"
                           f"&apiVersion=1.0.6"
                           f"&action=exportData"
                           f"&startDate={date}"
                           f"&exportFormat={export_format}")

        logging.info("Export Init URL: " + export_init_url)
        response = requests.get(export_init_url)
        response.raise_for_status()

        # Will hard fail if not present
        return response.json()["response"][0]["jobId"]

    def get_files(self, job_id, sleep_time=DEFAULT_SLEEP_SECONDS):
        export_retrieve_url = (f"http://www.leanplum.com/api?"
                               f"appId={self.app_id}"
                               f"&clientKey={self.bq_client_key}"
                               f"&apiVersion=1.0.6"
                               f"&action=getExportResults"
                               f"&jobId={job_id}")

        logging.info("Export URL: " + export_retrieve_url)
        loading = True

        while(loading):
            response = requests.get(export_retrieve_url)
            response.raise_for_status()

            state = response.json()['response'][0]['state']
            if (state == LeanplumExporter.FINISHED_STATE):
                logging.info("Export Ready")
                loading = False
            else:
                logging.info("Waiting for export to finish...")
                time.sleep(sleep_time)

        return response.json()['response'][0]['files']

    def save_files(self, file_uris, bucket_name, prefix, date, export_format, version):
        # Avoid calling storage.buckets.get since we don't need bucket metadata
        # https://github.com/googleapis/google-cloud-python/issues/3433
        bucket = self.gcs_client.bucket(bucket_name)
        datatypes = set()

        prefix = self.get_gcs_prefix(prefix, version, date)
        self.delete_gcs_prefix(bucket, prefix)

        for uri in file_uris:
            logging.info(f"Retrieving URI {uri}")

            parsed = self.filename_re.fullmatch(uri)
            if parsed is None:
                raise Exception((f"Expected uri matching {LeanplumExporter.FILENAME_RE}"
                                 f", but got {uri}"))

            datatype, index = parsed.group(1), parsed.group(2)
            local_filename = f"{datatype}/{index}.{export_format}"
            datatypes |= set([datatype])

            f = Path(local_filename)
            base_dir = Path(datatype)
            base_dir.mkdir(parents=True, exist_ok=True)

            with requests.get(uri, stream=True) as r:
                r.raise_for_status()
                with f.open("wb") as opened:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            opened.write(chunk)

            logging.info(f"Uploading to gs://{bucket_name}/{prefix}/{local_filename}")
            blob = bucket.blob(os.path.join(prefix, local_filename))
            blob.upload_from_filename(local_filename)

            f.unlink()
            base_dir.rmdir()

        return datatypes
