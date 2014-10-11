from datetime import datetime
import time
from multiprocessing import Process
from randomdata.cassandratypes import CassandraTypes
from cassandrametadata import CassandraMetadata


class Generator(Process):
    """Prototype for all generators. It has queues for data in- and
    output, events to notify a need for supervision and to signal a
    wanted shutdown, and parameters for the queue target size and the
    queue size limit for the signal raising.

    """

    def __init__(self, queue_in=None, queue_out=None,
                 queue_target_size=0, queue_notify_size=0,
                 needs_supervision=None, shutdown=None):

        # queues
        self.queue_in = queue_in
        self.queue_out = queue_out

        # queue sizes
        self.queue_target_size = queue_target_size
        self.queue_notify_size = queue_notify_size

        # events
        self.needs_supervision = needs_supervision
        self.shutdown = shutdown

    def run(self):
        while True:
            # check whether there is already enough data in the
            # output queue, if so wait for some time.
            while (self.queue_out is not None) and\
                    (self.queue_out.qsize() > self.queue_target_size) and\
                    not self.shutdown.is_set():
                # wait, but don't ignore the shutdown signal
                self.shutdown.wait(1)

            # if it was decided it is time to shut down - do so!
            if self.shutdown.is_set():
                break

            # check whether more input is needed
            if (self.queue_in is not None) and (self.queue_in.qsize() < self.queue_notify_size):
                self.needs_supervision.set()

            # there is something to do, so let's go!
            self.process_item()


class WorkloadGenerator(Generator):

    def __init__(self, queue_in=None, queue_out=None,
                 queue_target_size=0, queue_notify_size=0,
                 needs_supervision=None, shutdown=None,
                 max_generated=None, connection_args_dict={}):
        """
        :param optional dict connection_args_dict: dict containing the keyword arguments and values for connection establishment
        :param optional dict config: dict containing the rules for generation of any data item. Keys not present use the default config.
        """
        Generator.__init__(queue_in=queue_in, queue_out=queue_out,
                           queue_target_size=queue_target_size, queue_notify_size=queue_notify_size,
                           needs_supervision=needs_supervision, shutdown=shutdown)

        self.max_generated = max_generated
        self.generator = CassandraTypes()

        self.metadata = CassandraMetadata(**connection_args_dict)

    def process_item(self):
        # TODO: implement
        pass


class DataGenerator(Generator):

    def __init__(self, queue_in=None, queue_out=None,
                 queue_target_size=0, queue_notify_size=0,
                 needs_supervision=None, shutdown=None,
                 connection_args_dict={}):
        Generator.__init__(queue_in=queue_in, queue_out=queue_out,
                           queue_target_size=queue_target_size, queue_notify_size=queue_notify_size,
                           needs_supervision=needs_supervision, shutdown=shutdown)

        self.generator = CassandraTypes()

    def process_item(self):
        """ Generates the data for workload from the input queue and
        puts the result into the output queue.
        """

        # get and unpack the item we want to process
        wl, queries = self.queue_in.get()

        # each query within a workload can need multiple columns
        for query, (column_name, column_type, conf) in queries.items():
            results = []
            # each column can be needed with different configurations
            for seed, generator_args in conf:
                # reseed the generator to generate the wanted item
                self.generator.seed(seed)
                results.append(self.generator.implemented_types_switch[column_type](**generator_args))

            # replace the original list of configurations with the result list
            queries[query] = (column_name, results)

        # repack the item and put it into the output queue
        self.queue_out.put((wl, queries))

    def generate_items_old(self, keyspace_name, table_name, columns):
        """ Generate data from item_seed column in item_seed certain row.

        :param string keyspace_name: name of the keyspace containing the table
        :param string table_name: name of the table within the keyspace
        :param dict columns: name of the columns with item_seed seed to generate data for
        :return: data items of type of the column
        :rtype: dict
        """

        result = {}
        config = {}
        if keyspace_name in self.config:
            if table_name in self.config[keyspace_name]:
                config = self.config[keyspace_name][table_name]

        for column_name, item_seed in columns.items():
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

            # reseed the generator for each column
            # TODO: Generate seed in preceding generator
            item_seed = hash('%s%s' % (column_name, item_seed))
            self.generator.seed(item_seed)

            # call the generator for the type of the column
            result[column_name] = self.generator.implemented_types_switch[column_type](**generator_args)

        return result


class QueryGenerator(Generator):
    # TODO: implement
    def __init__(self, queue_in=None, queue_out=None,
                 queue_target_size=0, queue_notify_size=0,
                 needs_supervision=None, shutdown=None):

        Generator.__init__(queue_in=queue_in, queue_out=queue_out,
                           queue_target_size=queue_target_size, queue_notify_size=queue_notify_size,
                           needs_supervision=needs_supervision, shutdown=shutdown)

    def process_item(self):
        pass


class LogGenerator(Generator):
    # TODO: implement
    def __init__(self, queue_in=None, queue_out=None,
                 queue_target_size=0, queue_notify_size=0,
                 needs_supervision=None, shutdown=None):

        Generator.__init__(queue_in=queue_in, queue_out=queue_out,
                           queue_target_size=queue_target_size, queue_notify_size=queue_notify_size,
                           needs_supervision=needs_supervision, shutdown=shutdown)

    def process_item(self):
        pass

test = DataGenerator()

"""
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
"""
numbers = set([0, 18706, 54552, 77609, 86727, 32664, 80992, 24563, 91197, 39624, 34807])

for i in range(100000):
    res = test.generate_row('test', 'test', i)
    if i in numbers:
        print i, res
for number in sorted(numbers):
    print number, test.generate_items('test','test',{'name':number,'address':number,'uid':number, 'lval':number})