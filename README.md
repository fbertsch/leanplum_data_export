[![CircleCI](https://circleci.com/gh/mozilla/leanplum-data-export.svg?style=svg)](https://circleci.com/gh/mozilla/leanplum-data-export)

# Leanplum Data Export
This repository is the job to export Mozilla data from Leanplum and into BQ.

It's dockerized to run on GKE. To run locally:

```
pip install .
leanplum-data-export export-leanplum \
  --app-id $LEANPLUM_APP_ID \
  --client-key $LEANPLUM_CLIENT_KEY \
  --date 20190101 \
  --bucket gcs-leanplum-export \
  --table-prefix leanplum \
  --bq-dataset dev_external \
  --prefix dev
```

Doing it this way will, by default, use your local GCP credentials.
GCP only allows you to do this a few times

Alternatively, run in Docker.

First, create a service account with access to GCS and BigQuery.
Download a JSON key file and make it available in your
environment as `GCLOUD_SERVICE_KEY`. Then run:

```
bq mk leanplum
make run COMMAND="leanplum-data-export export-leanplum \
  --app-id $LEANPLUM_APP_ID \
  --client-key $LEANPLUM_CLIENT_KEY \
  --date 20190101 \
  --bucket gcs-leanplum-export \
  --table-prefix leanplum \
  --bq-dataset leanplum \
  --prefix dev"
```

That will create the dataset in BQ, download the files, and make
them available in BQ in that dataset as external tables.

## Development and Testing

While iterating on development, we recommend using virtualenv
to run the tests locally.

### Run tests locally

Install requirements locally:
```
python3 -m virtualenv venv
source venv/bin/activate
make install-requirements
```

Run tests locally:
```
pytest tests/
```

### Run tests in docker

You can run the tests just as CI does by building the container
and running the tests.

```
make clean && make build
make test
```

### Deployment

This project deploys automatically to GCR. The latest release is used to run the job.
