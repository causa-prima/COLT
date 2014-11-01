from config.configinterface import ConfigInterface
from connection.cassandraconnection import CassandraConnection


class CassandraConfig(ConfigInterface):

    def __init__(self, config_loc=None, connection=None):
        # get the schemata before construction the ConfigInterface, because it
        # automatically calls process_config, which needs the schemata
        self.schemata = {}
        self.get_schemata()

        self.connection_t = CassandraConnection()

        ConfigInterface.__init__(self, config_loc=config_loc,
                                 connection=connection)

        # string used to join arguments if needed, e.g. keyspace and table
        # name
        self.join_string = '@'

    def get_schemata(self):
        """ Receives schemata from cassandra and adds it to self.schema in a
        more convenient form, stripping unneeded information.
        """
        keyspaces = self.connection.cluster.metadata.keyspaces
        for ks_name, ks_metadata in keyspaces.items():
            # exclude cassandra system keyspaces
            if ks_name in ('system_traces', 'system'):
                continue
            self.schemata[ks_name] = {}
            for table_name, table_metadata in ks_metadata.tables.items():
                self.schemata[ks_name][table_name] = {}
                self.schemata[ks_name][table_name]['partition key'] = []
                self.schemata[ks_name][table_name]['clustering key'] = []

                for column in table_metadata.columns.values():
                    self.schemata[ks_name][table_name][column.name] = column.typestring

                for part_key in table_metadata.partition_key:
                    self.schemata[ks_name][table_name]['partition key'].append(part_key.name)
                for cl_key in table_metadata.clustering_key:
                    self.schemata[ks_name][table_name]['clustering key'].append(cl_key.name)

        for ks_name, val in keyspaces.items():
            for t_name, meta in val.tables.items():
                print '  ' * 1, 'table name: ', t_name
                print '  ' * 2, 'partition keys:'
                for part_key in meta.partition_key:
                    print '  ' * 3, part_key.name
                print '  ' * 2, 'clustering keys:'
                for cl_key in meta.clustering_key:
                    print '  ' * 3, cl_key.name

    def process_config(self):
        # TODO: add option to choose if database should be reinitialized, discarding all present data
        self.delete_old_schema()
        self.initialize_schema()
        self.get_schemata()

        # add the needed metadata to the workloads-section
        # TODO: add the needed metadata to the workloads-section
        for workload in self.config['workloads']:
            for query in workload['queries']:
                query = {}
                # Prepare the query, which also gets most needed metadata.
                # Notice that internal data of the prepared query is used,
                # which could change in future versions of the driver.
                prep_stmt = self.connection_t.prepare(query['query'])
                query['prepared_statement'] = prep_stmt
                # insert, delete, update and select all have 6 characters, and
                # as they have to be the first word in the query it is easy
                # to parse them
                query['type'] = prep_stmt.query_string[:6].lower()
                if query['type'] == 'insert':
                    query['chance'] = query['chance']
                attributes = []
                # iterate over the attributes of that query
                for ks, table, col_name, col_type in prep_stmt.column_metadata:
                    attribute_info = {}

                    # get the type name, append the subtypes for composite types
                    typename = col_type.typename
                    if len(col_type.subtypes) > 0:
                        typename += '<' + ','.join(t.typename for t in col_type.subtypes) + '>'
                    attribute_info['type'] = typename

                    # check what 'level' the attribute has
                    attribute_info['level'] = 'attribute'
                    if col_name in self.schemata[ks][table]['partition key']:
                        attribute_info['level'] = 'primary'
                    elif col_name in self.schemata[ks][table]['clustering key']:
                        attribute_info['level'] = 'partition'

                    # finally, copy the generator args
                    try:
                        attribute_info['generator args'] = self.config['schemata'][ks][table]['distributions'][col_name]
                    except KeyError:
                        attribute_info['generator args'] = {}
                    attributes.append(attribute_info)
                query['attributes'] = attributes

                # combine the keyspace and table name to get unique table names
                query['table'] = self.join_string.join([ks, table])



    def delete_old_schema(self):
        statement = 'DROP KEYSPACE IF EXISTS %s'
        for ks_name in self.config.schemate.keys():
            self.connection_t.execute_unprepared_stmt(statement % ks_name)

    def initialize_schema(self):
        for ks_name, ks_data in self.config.schemate.items():
            self.create_keyspace(ks_name, ks_data)

    def create_keyspace(self, ks_name, ks_data):
        """ Create the keyspace and tables defined in ks_def, which is a part
        of self.config, hence following the same format.

        :param dict ks_data: self.conf['schemata'][keyspace_name] where the keyspace to create is defined
        """
        self.connection_t.execute_unprepared_stmt(ks_data['definition'])
        for table_name, table_data in ks_data['tables'].items():
            self.create_table(table_data)
            # combine the keyspace and table name to get unique table names
            combined_name = self.join_string.join([ks_name, table_name])
            self.config['tables'][combined_name] = table_data

    def create_table(self, table_data):
        """ Creates the table defined in table_data, which is a part of
        self.config, hence following the same format

        :param dict table_data: self.conf['schemata'][keyspace_name]['tables'][tablename] where the table to create is defined
        """
        self.connection_t.execute_unprepared_stmt(table_data['definition'])

"""
this has to be in the config after parsing:

workloads:
    <name>:
	ratio: <num>
	queries: [
		  table: <str>
		  prepared_statement: <prepared_statement>
		  type: 'insert' | 'select' | 'update' | 'delete'		# how to get those? parsing? hopefully not..
		  chance: <float[0,1]>						# only if type == 'insert'
		  attributes: [
			       level: 'primary' | 'partition' | 'attribute'	# other names could be used - should they?
			       type: <data_type>
			       generator args: <dict of args for generator>	# can it be empty?
			      ]
		  ]
"""