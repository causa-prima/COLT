from cassandra.cluster import Cluster

def getKeyspaces(cluster):
    return cluster.metadata.keyspaces

def getSchemas(keyspaceMetadata):
    schema = {}
    for table in keyspaceMetadata.tables.values():
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