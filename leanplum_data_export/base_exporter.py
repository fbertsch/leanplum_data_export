import json
import os


class BaseLeanplumExporter(object):

    DROP_COLS = {"sessions": {"lat", "lon"}}
    SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "schemas/")

    def export(self, date, bucket, prefix, dataset,
               table_prefix, version, project):
        raise NotImplementedError()

    @classmethod
    def parse_schema(cls, data_type):
        try:
            drop_cols = cls.DROP_COLS.get(data_type, {})
            with open(os.path.join(cls.SCHEMA_DIR, f"{data_type}.schema.json"), "r") as schema_file:
                return [field for field in json.load(schema_file)
                        if field["name"] not in drop_cols]
        except FileNotFoundError:
            raise ValueError(f"Unrecognized table name encountered: {data_type}")
