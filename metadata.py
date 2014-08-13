from cassandra.cluster import Cluster

class CassandraMetadata(object):

    def __init__(self):
        self.cluster = Cluster()
        # TODO: is the session object needed?
        self.session = self.cluster.connect('test')
        self.keyspaces = self.cluster.metadata.keyspaces

    def __del__(self):
        self.cluster.shutdown()

    def getKeyspaceSchema(self, keyspaceName):
        schema = {}
        for table in self.keyspaces[keyspaceName].tables.values():
            schema[table.name] = table.columns
        return schema

"""
cluster = Cluster(metrics_enabled=True)
session = cluster.connect('test')


keyspaces = getKeyspaces(cluster)

for keyspace in keyspaces:
            print keyspace


testSchema = getSchemas(keyspaces['test'])

for table, columns in testSchema.items():
        print 'TABLE',table
        print '{:->50}'.format('')
        for column in columns.values():
            print '{:>25} | {}'.format(column.name, column.typestring)
        print


cluster.shutdown()
"""