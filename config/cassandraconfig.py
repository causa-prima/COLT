from config.configinterface import ConfigInterface
from connection.cassandraconnection import CassandraConnection


class CassandraConfig(ConfigInterface):

    def __init__(self, config_loc=None, connection=None):
        # get the schemata before construction the ConfigInterface, because it
        # automatically calls process_config, which needs the schemata
        self.schemata = {}
        self.get_schema()

        self.connection_t = CassandraConnection()

        ConfigInterface.__init__(self, config_loc=config_loc,
                                 connection=connection)

    def get_schema(self):
        """ Receives schemata from cassandra and adds it to self.schema in a
        more convenient form.
        """
        keyspaces = self.connection.cluster.metadata.keyspaces
        for ks_name, ks_metadata in keyspaces.items():
            # exclude cassandra system keyspaces
            if ks_name in ('system_traces', 'system'):
                continue
            self.schemata[ks_name] = {}
            for table_name, table_metadata in ks_metadata.tables.items():
                self.schemata[ks_name][table_name] = {}
                for column in table_metadata.columns.values():
                    self.schemata[ks_name][table_name][column.name] = column.typestring

    def process_config(self):
        # check if database should be reinitialized, dumping all present data
        try:
            delete_old = self.config['config']['delete old']
        except KeyError:
            delete_old = True
        if delete_old:
            self.delete_old_schema()
            self.initialize_schema()

        # check if all defined keyspaces and tables are already in the database
        # and create them if they are not
        for ks_name, ks_data in self.config['schemata'].items():
            if ks_name in self.schemata:
                for table_name, table_data in ks_data['tables']:
                    if table_name in self.schemata[ks_name]:
                        continue
                    else:
                        self.create_table(table_data)
            else:
                self.create_keyspace(ks_data)

        # refresh the schema data
        # TODO: needed?
        self.get_schema()

        # add the needed metadata to the workloads-section
        # TODO: add the needed metadata to the workloads-section

    def delete_old_schema(self):
        statement = 'DROP KEYSPACE IF EXISTS %s'
        for ks_name in self.config.schemate.keys():
            self.connection_t.execute_unprepared_stmt(statement % ks_name)

    def initialize_schema(self):
        for ks_name, ks_data in self.config.schemate.items():
            self.connection_t.execute_unprepared_stmt(ks_data['definition'])
            for table_name, table_data in ks_data['tables']:
                self.connection_t.execute_unprepared_stmt(table_data['definition'])

    def create_keyspace(self, ks_data):
        """ Create the keyspace and tables defined in ks_def, which is a part
        of self.config, hence following the same format

        :param dict ks_data: self.conf['schemata'][keyspace_name] where the keyspace to create is defined
        """
        self.connection_t.execute_unprepared_stmt(ks_data['definition'])
        for table_data in ks_data['tables'].values():
            self.create_table(table_data)

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