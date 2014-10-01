from datetime import datetime
import time
from randomdata.cassandratypes import CassandraTypes
from cassandrametadata import CassandraMetadata


class Generator(object):
    def __init__(self, seed=None, connection_args_dict={}, config={}):
        """
        :param hashable seed: seed for the underlying PRNG.
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

    def whole_table_generator(self, keyspace_name, table_name, num_rows):
        """ Generator for a table with all its entries, yielding one row at a time.

        :param keyspace_name: name of the keyspace containing the table
        :param table_name: name of the table within the keyspace
        :param num_rows: number of rows to generate
        :return: single row dict
        :rtype: generator
        """
        n = 0
        while n < num_rows:
            yield self.generate_row(keyspace_name, table_name, row_number=n)
            n += 1

    def generate_row(self, keyspace_name, table_name, row_number=0):
        """ Generator one row of a table.

        :param string keyspace_name: name of the keyspace containing the table
        :param string table_name: name of the table within the keyspace
        :param int row_number: number of the row to generate. default = 0
        :return: dict with column names and corresponding values
        :rtype: dict
        """

        res = {}
        config = {}
        if keyspace_name in self.config:
            if table_name in self.config[keyspace_name]:
                config = self.config[keyspace_name][table_name]

        for column, column_type in self.metadata.schema[keyspace_name][table_name].items():
            # reseed the generator for each column so we can
            # re-generate data for each row easily
            a = hash('%s%s' % (column, row_number))
            self.generator.seed(a)

            # get config arguments for the generator (if they exist)
            generator_args = {}
            if column in config:
                generator_args = config[column]

            # parse type(s) of collection types
            if column_type[-1] is '>':
                # strip the last char from column_type string and split it at <
                column_type, _, elem_type = column_type[:-1].partition('<')

                # maps have two data types for key and value
                if column_type[0] is 'm':
                    key_type, _, elem_type = elem_type.partition(', ')
                    generator_args['key_type'] = key_type

                generator_args['elem_type'] = elem_type

            # call the generator for the type of the column
            res[column] = self.generator.implemented_types_switch[column_type](**generator_args)

        return res

    def generate_row_items(self, keyspace_name, table_name, columns):
        """ Generate data from a column in a certain row.

        :param string keyspace_name: name of the keyspace containing the table
        :param string table_name: name of the table within the keyspace
        :param dict columns: name of the columns with a list of seeds to generate data for
        :return: data items of type of the column
        :rtype: dict
        """

        res = {}
        config = {}
        if keyspace_name in self.config:
            if table_name in self.config[keyspace_name]:
                config = self.config[keyspace_name][table_name]

        for column_name, seed_list in columns.items():
            try:
                column_type = self.metadata.schema[keyspace_name][table_name][column_name]
            except KeyError:
                msg = 'Column name "{}" not found in keyspace "{}", table "{}"'.format(
                                                                                column_name,
                                                                                keyspace_name,
                                                                                table_name)
                raise LookupError(msg)

            # get config arguments for the generator (if they exist)
            generator_args = {}
            if column_name in config:
                generator_args = config[column_name]

            # parse type(s) of collection types
            if column_type[-1] is '>':
                # strip the last char from column_type string and split it at <
                column_type, _, elem_type = column_type[:-1].partition('<')

                # maps have two data types for key and value
                if column_type[0] is 'm':
                    key_type, _, elem_type = elem_type.partition(', ')
                    generator_args['key_type'] = key_type

                generator_args['elem_type'] = elem_type

            # TODO: user-defined types introduced in C* 2.1

            for seed in seed_list:
                # reseed the generator for each column
                a = hash('%s%s' % (column_name, seed))
                self.generator.seed(a)

                # call the generator for the type of the column
                res[column_name] = self.generator.implemented_types_switch[column_type](**generator_args)

        return res

    def generate_row_old(self, keyspace_name, table_name, row_number=0):
        """ Generator one row of a table.

        :param string keyspace_name: name of the keyspace containing the table
        :param string table_name: name of the table within the keyspace
        :param int row_number: number of the row to generate. default = 0
        :return: dict with column names and corresponding values
        :rtype: dict
        """

        # reseed the generator for each row so we can
        # re-generate data for each row easily
        self.generator.seed(self.seed+row_number)

        res = {}
        config = {}
        later = {}
        if keyspace_name in self.config:
            if table_name in self.config[keyspace_name]:
                config = self.config[keyspace_name][table_name]

        for column, column_type in self.metadata.schema[keyspace_name][table_name].items():
            if row_number == 0:
                print column, 'state:', self.generator.getstate()[1]
            # get config arguments for the generator (if they exist)
            generator_args = {}
            if column in config:
                generator_args = config[column]

            # parse type(s) of collection types
            if column_type[-1] is '>':
                # strip the last char from column_type string and split it at <
                column_type, _, elem_type = column_type[:-1].partition('<')

                # maps have two data types for key and value
                if column_type[0] is 'm':
                    key_type, _, elem_type = elem_type.partition(', ')
                    generator_args['key_type'] = key_type

                generator_args['elem_type'] = elem_type

                # add collection type to the dict for later computation
                # and continue with next column
                later[column] = (column_type, generator_args)
                continue

            # call the generator for the type of the column
            res[column] = self.generator.implemented_types_switch[column_type](**generator_args)

        # process the dict of collection types
        # this makes it much more easy to regenerate
        # single items in a previously generated row
        for column, (column_type, generator_args) in later.items():
            res[column] = self.generator.implemented_types_switch[column_type](**generator_args)

        return res

    def generate_row_items_old(self, keyspace_name, table_name, column_names, row_number):
        """ Generate data from a column in a certain row.

        :param string keyspace_name: name of the keyspace containing the table
        :param string table_name: name of the table within the keyspace
        :param set column_names: name of the columns to generate data from
        :param int row_number: number of the row the column data is in
        :return: data items of type of the column
        :rtype: dict
        """

        # jump to the state the PRNG was in when generating the row
        self.generator.seed(self.seed+row_number)

        res = {}
        config = {}
        later = {}
        skip = 0
        if keyspace_name in self.config:
            if table_name in self.config[keyspace_name]:
                config = self.config[keyspace_name][table_name]

        for column, column_type in self.metadata.schema[keyspace_name][table_name].items():
            if row_number == 0:
                print column,'state:', self.generator.getstate()[1]

            # check if column needs to be generated or is a collection type
            if column_type[-1] is not '>':
                if column not in column_names:
                    if row_number == 0:
                        print 'skipping', column
                    self.generator.jumpahead(1)
                    #skip += 1
                    continue
                else:
                    # get config arguments for the generator (if they exist)
                    generator_args = {}
                    if column in config:
                        generator_args = config[column]

                    # let the PRNG jump over non-generated items
                    if skip:
                        self.generator.jumpahead(skip+1)
                    # call the generator for the type of the column
                    res[column] = self.generator.implemented_types_switch[column_type](**generator_args)
                    skip = 0
                    column_names.remove(column)
            else:
                generator_args = {}
                # parse type(s) of collection types
                # strip the last char from column_type string and split it at <
                column_type, _, elem_type = column_type[:-1].partition('<')

                # maps have two data types for key and value
                if column_type[0] is 'm':
                    key_type, _, elem_type = elem_type.partition(', ')
                    generator_args['key_type'] = key_type

                generator_args['elem_type'] = elem_type

                # add collection type to the dict for later computation
                # and continue with next column
                later[column] = (column_type, generator_args)

        # process the dict of collection types if needed
        # all present collection types have to be generated
        # until all needed columns have been generated
        if len(column_names):
            for column, (column_type, generator_args) in later.items():
                if column in column_names:
                    res[column] = self.generator.implemented_types_switch[column_type](**generator_args)
                    column_names.remove(column)
                else:
                    self.generator.implemented_types_switch[column_type](**generator_args)

        if len(column_names):
            unfound_columns = ", ".join(str(e) for e in column_names)
            msg = 'Column names not found in keyspace "{}", table "{}": {}'.format(
                                                                                keyspace_name,
                                                                                table_name,
                                                                                unfound_columns)
            raise Warning(msg)

        return res


test = Generator()


def testprogramm():
    for _ in test.whole_table_generator('test', 'insanitytest', 100000):
        pass
    #for _ in range(10000):
    #    test.generate_row('test','insanitytest')
import cProfile
cProfile.run("testprogramm()", sort='time')


'''
from pycallgraph import PyCallGraph
from pycallgraph.output import GraphvizOutput
from pycallgraph import Config

config = Config(max_depth=100, include_stdlib=True)
with PyCallGraph(output=GraphvizOutput(output_file='/home/causa-prima/callgraph.png'), config=config):
    for _ in test.whole_table_generator('test', 'insanitytest', 100):
        pass
'''
'''
numbers = set([0, 18706, 54552, 77609, 86727, 32664, 80992, 24563, 91197, 39624, 34807])

for i in range(100000):
    res = test.generate_row('test', 'test', i)
    if i in numbers:
        print i, res
for number in sorted(numbers):
    print number, test.generate_row_items('test','test',{'name':[number],'address':[number],'uid':[number]})
'''