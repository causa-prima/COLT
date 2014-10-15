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
                 needs_supervision=None, shutdown=None,
                 config=None):

        # queues
        self.queue_in = queue_in
        self.queue_out = queue_out

        # queue sizes
        self.queue_target_size = queue_target_size
        self.queue_notify_size = queue_notify_size

        # events
        self.needs_supervision = needs_supervision
        self.shutdown = shutdown

        # configuration
        self.config = config


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
                 config=None, key_structs=None):
        Generator.__init__(queue_in=queue_in, queue_out=queue_out,
                           queue_target_size=queue_target_size, queue_notify_size=queue_notify_size,
                           needs_supervision=needs_supervision, shutdown=shutdown, config=config)

        self.key_structs = key_structs
        self.generator = CassandraTypes()

        # aggregate the chances and map the chances
        # of each workload into [0,ratio_sum]
        self.ratio_sum = 0
        self.ratio_nums = {}
        for workload_name, workload_data in config['workloads'].items():
            self.ratio_sum += workload_data['ratio']
            self.ratio_nums[self.ratio_sum] = workload_name

    def process_item(self):
        # choose a workload to work on by picking
        # a random int between 0 and ratio_sum
        choice = self.generator.randint(0, self.ratio_sum)
        choice = max(key for key in self.ratio_nums if key <= choice)

        # get the chosen workload
        workload_name = self.ratio_nums[choice]
        workload = self.config['workloads'][workload_name]

        #output:(workload,[queries]) with queries = [(new?,(column_type, seed, generator_args))]
        queries = []
        for query in workload['queries']:
            q = []
            # we need the bitmap of seeds that were used as primary keys
            keyspace = query['keyspace']
            table = query['table']
            key_struct = self.key_structs[keyspace][table]
            # check if a new data item might be generated
            if query['type'] == 'insert':
                # lock the bitmap for the table the item will be stored in
                with key_struct.lock:
                    primary_seed = len(key_struct.bitmap)
                    cluster_seed = primary_seed
                    # seed the generator to produce regeneratable results
                    self.generator.seed(primary_seed)
                    # determine if this seed just generates
                    # a new cluster for an old primary key
                    new_cluster = query['chance'] > self.generator.random()
                    if not new_cluster:
                        # We need an old key that created a completely new
                        # item, so we randomly iterate over old keys until we
                        # find one that did. If no key was ever used to create
                        # a completely new item, we fall back to seed zero,
                        # which always generates a completely new item.
                        while True:
                            cluster_seed = self.generator.randrange(0, primary_seed+1)
                            if key_struct.bitmap[cluster_seed] or cluster_seed == 0:
                                break

                    # Tell the other processes what happened by appending the
                    # choice we made to the bitmap. The or-part makes sure that
                    # bitmap[0] is always 1, i.e. seed zero always generates a
                    # completely new item
                    key_struct.bitmap.append(new_cluster or cluster_seed == primary_seed)

            elif query['type'] == 'select':
                # Both a primary key and a cluster key are needed. Choose a
                # random old seed and look what happened with that seed. If
                # it generated a new cluster for another primary key, get that
                # key by following the steps that originally led to that key.
                with key_struct.lock:
                    primary_seed = len(key_struct.bitmap)+1
                    primary_seed = self.generator.randrange(0, primary_seed)
                    cluster_seed = primary_seed
                    # if this seed did not produce a completely new item the
                    # primary key it
                    if not key_struct.bitmap[primary_seed]:
                        self.generator.seed(primary_seed)
                        # When generating a new cluster it is first tested if
                        # a new cluster will be generated by using a random
                        # number, so we need to advance the generator one step.
                        self.generator.random()
                        # now we can search for the primary key that was used
                        while True:
                            cluster_seed = self.generator.randrange(0, primary_seed+1)
                            if key_struct.bitmap[cluster_seed] or cluster_seed == 0:
                                break
            else:
                msg = 'unsupported query type %s' % query[type]
                raise NotImplementedError(msg)

            # In case the query uses a cluster of a primary key the seeds have
            # to be switched, otherwise they are identical.
            if primary_seed != cluster_seed:
                primary_seed, cluster_seed = cluster_seed, primary_seed
            # Finally iterate over all the attributes of this query and append
            # the needed metadata to the list of queries.
            for attribute in query['attributes']:
                if attribute['level'] == 'primary':
                    seed = primary_seed
                else:
                    seed = cluster_seed
                data = (attribute['type'], seed, attribute['generator_args'])
                q.append((query['type'] == 'insert', data))
            # append the data for this query to the
            # list of queries for that workload
            queries.append(q)

        # put the workload with its data into the queue
        self.queue_out.put((workload, queries))


class DataGenerator(Generator):

    def __init__(self, queue_in=None, queue_out=None,
                 queue_target_size=0, queue_notify_size=0,
                 needs_supervision=None, shutdown=None,
                 config=None, connection_args_dict={}):
        Generator.__init__(queue_in=queue_in, queue_out=queue_out,
                           queue_target_size=queue_target_size, queue_notify_size=queue_notify_size,
                           needs_supervision=needs_supervision, shutdown=shutdown, config=config)

        self.generator = CassandraTypes()

    def process_item(self):
        """ Generates the data for workload from the input queue and
        puts the result into the output queue.
        """

        # get and unpack the item we want to process
        workload, queries = self.queue_in.get()

        # Each workload could have multiple queries. Each query could
        # need multiple columns. Each column could be needed more
        # than once. Each column instance could be needed with
        # different configurations.
        workload_data = []
        for query in queries:
            results = []
            for new, (type, seed, generator_args) in query:

                # reseed the generator to generate the wanted item
                self.generator.seed(seed)
                try:
                    val = self.generator.methods_switch[type](**generator_args)
                except KeyError:
                    msg = "generator for type %s not implemented!" % type
                    raise NotImplementedError(msg)
                results.append((new, val))

            # replace the original list of configurations with the result list
            workload_data.append(results)

        # repack the item and put it into the output queue
        self.queue_out.put((workload, workload_data))

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
            result[column_name] = self.generator.methods_switch[column_type](**generator_args)

        return result


class QueryGenerator(Generator):
    # TODO: implement
    def __init__(self, queue_in=None, queue_out=None,
                 queue_target_size=0, queue_notify_size=0,
                 needs_supervision=None, shutdown=None,
                 config=None, max_inserted=None):

        Generator.__init__(queue_in=queue_in, queue_out=queue_out,
                           queue_target_size=queue_target_size, queue_notify_size=queue_notify_size,
                           needs_supervision=needs_supervision, shutdown=shutdown, config=config)

    def process_item(self):
        pass


class LogGenerator(Generator):
    # TODO: implement
    def __init__(self, queue_in=None, queue_out=None,
                 queue_target_size=0, queue_notify_size=0,
                 needs_supervision=None, shutdown=None,
                 config=None):

        Generator.__init__(queue_in=queue_in, queue_out=queue_out,
                           queue_target_size=queue_target_size, queue_notify_size=queue_notify_size,
                           needs_supervision=needs_supervision, shutdown=shutdown, config=config)

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