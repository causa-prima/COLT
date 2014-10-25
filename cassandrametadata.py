from connection.cassandraconnection import CassandraConnection


class CassandraMetadata(object):
    def __init__(self, **connection_kwargs):
        self.connection = CassandraConnection(**connection_kwargs)
        self.schema = {}
        self.get_schema()

    def get_schema(self):
        """ Adds all needed schema data for the generation of random data to self.schema.
        """
        for ks_name, ks_metadata in self.connection.cluster.metadata.keyspaces.items():
            # exclude cassandra system keyspaces
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

#print CassandraMetadata()
"""
this has to be in the config after parsing:

workloads:
    <name>:
	ratio: <num>
	queries: [
		  keyspace: <str>
		  table: <str>
		  prepared_statement: <prepared_statement>
		  type: 'insert' | 'select' | 'update' | 'delete'		# how to get those? parsing? hopefully not..
		  chance: <float[0,1]>						# only if type == 'insert'
		  attributes: [
			       level: 'primary' | 'partition' | 'attribute'	# other names could be used - should they?
			       type: <data_type>
			       generator-args: <dict of args for generator>	# can it be empty?
			      ]
		  ]
"""
