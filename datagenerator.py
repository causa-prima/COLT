from datetime import datetime
from collections import defaultdict
from randomdata.cassandratypes import CassandraTypes
from cassandrametadata import CassandraMetadata


class Generator(object):
    def __init__(self, seed=None, connection_args_dict={}, config={}):
        """
        :param seed:
        :param optional dict connection_args_dict: dict containing the keyword arguments and values for connection establishment
        :param optional dict config: dict containing the rules for generation of any data item. Keys not present use the default config.
        """
        self.generator = CassandraTypes(seed)

        # if no seed is given generate one
        if seed is None:
            # 30268*30306*30322 is the maximum number of states of the PRNG (??)
            seed = self.generator.pyint(0, (30268 * 30306 * 30322) - 1)
            self.generator.seed(seed)
            self.config = config

        self.seed = seed
        self.metadata = CassandraMetadata(**connection_args_dict)

    def whole_table_generator(self, keyspace_name, table_name, rows):
        """ Generator for a table with all its entries, yielding one row at a time.

        :param keyspace_name: name of the keyspace containing the table
        :param table_name: name of the table within the keyspace
        :param rows: number of rows to generate
        :return: single row dict
        :rtype: generator
        """
        n = 0
        while n < rows:
            yield self.generate_single_row(keyspace_name, table_name)
            n += 1

    def generate_single_row(self, keyspace_name, table_name):
        """ Generator one row of a table.

        :param keyspace_name: name of the keyspace containing the table
        :param table_name: name of the table within the keyspace
        :return: dict with column names and corresponding values
        :rtype: dict
        """
        res = {}
        config = {}
        if keyspace_name in self.config:
            if table_name in self.config[keyspace_name]:
                config = self.config[keyspace_name][table_name]

        for column, column_type in self.metadata.schema[keyspace_name][table_name].items():
            print column, column_type
            if column_type == 'set<text>':
                continue
            # get config arguments for the generator (if they exist)
            generator_args = {}
            if column in config:
                generator_args = config[column]

            # call the generator for the type of the column
            res[column] = self.generator.implemented_types_switch[column_type](**generator_args)
            print res[column]

        return res


"""rndtest = CassandraTypes(0)

for key in rndtest.implemented_types_switch.keys():
    val = rndtest.implemented_types_switch[key]()
    print key
    print val
    if key in ('uuid', 'timeuuid'):
        print datetime.fromtimestamp((val.time - 0x01b21dd213814000L) * 100 / 1e9)
    print
    """

test = Generator()
print test.generate_single_row('test','user')