# -*- coding: utf-8 -*-

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import click
import logging
import sys

from leanplum_data_export.export import LeanplumExporter
from leanplum_data_export.export_streaming import StreamingLeanplumExporter


@click.command()
@click.option("--app-id", default=None)
@click.option("--client-key", default=None)
@click.option("--date", required=True)
@click.option("--bucket", required=True)
@click.option("--prefix", default="")
@click.option("--bq-dataset", required=True)
@click.option("--project", required=True)
@click.option("--table-prefix", default=None)
@click.option("--version", default=1)
@click.option("--streaming/--no-streaming", default=False)
@click.option("--s3-bucket", default=None)
def export_leanplum(app_id, client_key, date, bucket, prefix,
                    bq_dataset, table_prefix, version, project,
                    streaming, s3_bucket):
    if not streaming:
        if app_id is None or client_key is None:
            raise ValueError("--app-id and --client-key arguments must be "
                             "specified for historical export")
        exporter = LeanplumExporter(project, app_id, client_key)
        exporter.export(date, bucket, prefix, bq_dataset, table_prefix, version)
    else:
        if s3_bucket is None:
            raise ValueError("--s3-bucket must be specified for streaming export")
        exporter = StreamingLeanplumExporter(project)
        exporter.export(date, s3_bucket, bucket, prefix, bq_dataset, table_prefix, version)


@click.group()
def main(args=None):
    """Command line utility"""
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)


main.add_command(export_leanplum)


if __name__ == "__main__":
    sys.exit(main())
