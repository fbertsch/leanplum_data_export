class BaseLeanplumExporter(object):

    def export(self, date, bucket, prefix, dataset,
               table_prefix, version, project):
        raise NotImplementedError()
