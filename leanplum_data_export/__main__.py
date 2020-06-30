# -*- coding: utf-8 -*-

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import click
import logging
import sys

from leanplum_data_export.export import LeanplumExporter


@click.command()
@click.option("--date", required=True)
@click.option("--bucket", required=True)
@click.option("--prefix", default="")
@click.option("--bq-dataset", required=True)
@click.option("--project", required=True)
@click.option("--table-prefix", default=None)
@click.option("--version", default=1)
@click.option("--s3-bucket", required=True,
              help="Name of the bucket to retrieve exported streaming data from")
@click.option("--clean/--no-clean", default=False,
              help="A clean run will reprocess the entire day")
def export_leanplum(date, bucket, prefix, bq_dataset, table_prefix,
                    version, project, s3_bucket, clean):
        exporter = LeanplumExporter(project)
        exporter.export(date, s3_bucket, bucket, prefix, bq_dataset, table_prefix, version, clean)


@click.group()
def main(args=None):
    """Command line utility"""
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)


main.add_command(export_leanplum)


if __name__ == "__main__":
    sys.exit(main())
