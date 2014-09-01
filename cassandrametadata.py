from connection.cassandraconnection import CassandraConnection


class CassandraMetadata(object):
    def __init__(self):
        self.connection = CassandraConnection()
        self.schema = {}
        self.getSchema()

    def getSchema(self):
        """ Adds all needed schema data for the generation of random data to self.schema.
        """
        for ks_name, ks_metadata in self.connection.cluster.metadata.keyspaces.items():
            if ks_name in ('system_traces', 'system'):
                continue
            self.schema[ks_name] = {}
            for table_name, table_metadata in ks_metadata.tables.items():
                self.schema[ks_name][table_name] = {}
                for column in table_metadata.columns.values():
                    self.schema[ks_name][table_name][column.name] = column.typestring

    # defining the output when calling the print method of a CassandraMetadata object
    def __str__(self):
        res = []
        for keyspace, tables in self.schema.items():
            res.append('KEYSPACE "' + keyspace + '"')
            res.append('{:*>50}'.format(''))
            res.append('')

            for table, columns in tables.items():
                    res.append('TABLE "' + table + '"')
                    res.append('{:->50}'.format(''))
                    for column, typestring in columns.items():
                        res.append('{:>25} | {}'.format(column, typestring))
                    res.append('')
            res.append('')

        return '\n'.join(res)

print CassandraMetadata()
