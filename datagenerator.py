from multiprocessing import Process
from time import time
from datetime import datetime

from randomdata.cassandratypes import CassandraTypes


class Generator(Process):
    """Prototype for all generators. It has queues for data in- and
    output, events to notify a need for supervision and to signal a
    wanted shutdown, and parameters for the queue target size and the
    queue size limit for the signal raising.

    """

    def __init__(self, queue_in=None, queue_out=None,
                 queue_target_size=0, queue_notify_size=0,
                 needs_more_input=None, shutdown=None,
                 config=None):

        # queues
        self.queue_in = queue_in
        self.queue_out = queue_out

        # queue sizes
        self.queue_target_size = queue_target_size
        self.queue_notify_size = queue_notify_size

        # events
        self.needs_more_input = needs_more_input
        self.shutdown = shutdown

        # configuration
        self.config = config


    def run(self):
        while True:
            # Check whether there is already enough data in the
            # output queue, and wait while there is.
            while (self.queue_out is not None) and\
                    (self.queue_out.qsize() > self.queue_target_size) and\
                    not self.shutdown.is_set():
                # wait for more input, but don't ignore the shutdown signal
                # TODO: How long should be waited?
                self.shutdown.wait(.01)

            # if it was decided it is time to shut down - do so!
            if self.shutdown.is_set():
                break

            # check whether more input is needed
            if (self.queue_in is not None) and\
                    (self.queue_in.qsize() < self.queue_notify_size):
                self.needs_more_input.set()

            # there is something to do, so let's go!
            self.process_item()


class WorkloadGenerator(Generator):

    def __init__(self, queue_in=None, queue_out=None,
                 queue_target_size=0, queue_notify_size=0,
                 needs_more_input=None, shutdown=None,
                 config=None, key_structs=None, max_inserted=None):
        Generator.__init__(queue_in=queue_in, queue_out=queue_out,
                           queue_target_size=queue_target_size,
                           queue_notify_size=queue_notify_size,
                           needs_more_input=needs_more_input,
                           shutdown=shutdown, config=config)

        self.key_structs = key_structs
        self.max_inserted = max_inserted
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
                    partition_seed = len(key_struct.bitmap)
                    cluster_seed = partition_seed
                    # seed the generator to produce regeneratable results
                    self.generator.seed(partition_seed)
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
                            cluster_seed = self.generator.randrange(0, partition_seed+1)
                            if key_struct.bitmap[cluster_seed] or cluster_seed == 0:
                                break

                    # Tell the other processes what happened by appending the
                    # choice we made to the bitmap. The or-part makes sure that
                    # bitmap[0] is always 1, i.e. seed zero always generates a
                    # completely new item
                    key_struct.bitmap.append(new_cluster or cluster_seed == partition_seed)

            # query types other than insert need the partition and cluster key
            # to access specific data. Note that:
            #  - update takes an old item and inserts the same data that it
            #    already had, because we would need another data structure
            #    to mark an item as "updated" and record the changes
            #  - deleted items are not recorded in any data structure, so they
            #    might be part of other queries and produce errors or empty
            #    result sets
            elif query['type'] in ('select', 'update', 'delete'):
                # Both a partition key and a cluster key are needed. Choose a
                # random old seed and look what happened with that seed. If it
                # generated a new cluster for another partition key, get that
                # key by following the steps that originally led to that key.

                # Note that this number does not necessarily represent the seed
                # that was used to generate the new item. For further
                # information check the LogGenerator's process_item method.
                with self.max_inserted.get_lock():
                    partition_seed = self.max_inserted.value

                partition_seed = self.generator.randrange(0, partition_seed)
                cluster_seed = partition_seed

                with key_struct.lock:
                    # if this seed did not produce a completely new item the
                    # partition key it did produce a new cluster for is needed
                    if not key_struct.bitmap[partition_seed]:
                        self.generator.seed(partition_seed)
                        # When generating a new cluster it is first tested if
                        # a new cluster will be generated by using a random
                        # number, so we need to advance the generator one step.
                        self.generator.random()
                        # now we can search for the partition key that was used
                        # notice this takes 1/chance steps on average
                        while True:
                            cluster_seed = self.generator.randrange(0, partition_seed+1)
                            if key_struct.bitmap[cluster_seed] or cluster_seed == 0:
                                break
            else:
                msg = 'unsupported query type %s' % query[type]
                raise NotImplementedError(msg)

            # In case the query uses a cluster of a partition key the seeds have
            # to be switched, otherwise they are identical.
            if partition_seed != cluster_seed:
                partition_seed, cluster_seed = cluster_seed, partition_seed
            # Finally iterate over all the attributes of this query and append
            # the needed metadata to the list of queries.
            for attribute in query['attributes']:
                if attribute['level'] == 'partition':
                    seed = partition_seed
                else:
                    seed = cluster_seed
                data = (attribute['type'], seed, attribute['generator_args'])
                # append an object with a marker for new objects and the data
                # to the list of queries
                q.append((query['type'] == 'insert', data))
            # append the data for this query to the
            # list of queries for that workload
            queries.append(q)

        # put the workload with its data into the queue
        self.queue_out.put((workload, queries))


class DataGenerator(Generator):

    def __init__(self, queue_in=None, queue_out=None,
                 queue_target_size=0, queue_notify_size=0,
                 needs_more_input=None, shutdown=None,
                 config=None, connection_args_dict={}):
        Generator.__init__(queue_in=queue_in, queue_out=queue_out,
                           queue_target_size=queue_target_size,
                           queue_notify_size=queue_notify_size,
                           needs_more_input=needs_more_input,
                           shutdown=shutdown, config=config)

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
            values = []
            for new, (type, seed, generator_args) in query:

                # reseed the generator to generate the wanted item
                self.generator.seed(seed)
                try:
                    val = self.generator.methods_switch[type](**generator_args)
                except KeyError:
                    msg = "generator for type %s not implemented!" % type
                    raise NotImplementedError(msg)
                values.append(val)

            # replace the original list of configurations with the result list
            workload_data.append((new, values))

        # repack the item and put it into the output queue
        self.queue_out.put((workload, workload_data))


class QueryGenerator(Generator):

    def __init__(self, queue_in=None, queue_out=None,
                 queue_target_size=0, queue_notify_size=0,
                 needs_more_input=None, shutdown=None,
                 config=None, connection=None):

        Generator.__init__(queue_in=queue_in, queue_out=queue_out,
                           queue_target_size=queue_target_size,
                           queue_notify_size=queue_notify_size,
                           needs_more_input=needs_more_input,
                           shutdown=shutdown, config=config)
        # TODO: Are multiple connection objects needed or is it sufficient to share one over all QueryGenerators?
        self.connection = connection

    def process_item(self):
        """ Generates queries with data from the input queue,
        submits queries to the DB and puts a object that will
        eventually receive the result into the output queue.
        """

        # get and unpack the item we want to process
        workload, workload_data = self.queue_in.get()

        query_num = 0

        for new, query_values in workload_data:
            # for each query, get the prepared statement and call the connection
            # object to bind and execute the query, which automatically puts
            # resulting execution times into the out_queue
            prep_stmnt = workload['queries'][query_num]['prepared_statement']
            self.connection.execute(prep_stmnt, query_values, self.queue_out,
                                    metadata=(workload, query_num, new))

            query_num += 1


class LogGenerator(Generator):

    def __init__(self, queue_in=None, queue_out=None,
                 queue_target_size=0, queue_notify_size=0,
                 needs_more_input=None, shutdown=None,
                 config=None, max_inserted=None, logs=None,
                 queue_max_time=None, needs_more_processes=None):

        Generator.__init__(queue_in=queue_in, queue_out=queue_out,
                           queue_target_size=queue_target_size,
                           queue_notify_size=queue_notify_size,
                           needs_more_input=needs_more_input,
                           shutdown=shutdown, config=config)

        self.max_inserted = max_inserted
        # dict to log the execution times as datetime.timedelta
        self.logs = logs
        self.queue_max_time = queue_max_time
        self.needs_more_processes = needs_more_processes

    def process_item(self):
        err, start, end, (workload, query_num, new) = self.queue_in.get()
        now = time()
        time_in_queue = (datetime.fromtimestamp(now) - end).seconds
        # check whether more LogGenerator processes are needed
        if time_in_queue > self.queue_max_time or\
                        self.queue_in.qsize() > self.queue_target_size:
            self.needs_more_processes.set()

        now = int(now)
        # do not log execution times of errors
        if not err:
            with self.logs.lock:
                try:
                    self.logs.latencies[now] += end - start
                    self.logs.queries[now] += 1
                except KeyError:
                    self.logs.latencies[now] = end - start
                    self.logs.queries[now] = 1

        # If a new item was generated, the max_inserted counter needs to be
        # incremented for the WorkloadGenerators to notice this.
        # Note that this number does not necessarily represent the seed that
        # was used to generate the new item, as queries should happen
        # asynchronously. To guarantee that max_inserted equals the maximum
        # seed related to an item in the database, another way of storing this
        # information would be needed. This would introduce more complexity and
        # overhead. The chosen approach should work well enough as long as no
        # errors occur when inserting new data, hence this case should be
        # considered.
        if new:
            if err:
                msg = 'New item should have been inserted, but an error occured.'
                raise Warning(msg)
            with self.max_inserted.get_lock():
                self.max_inserted.value += 1




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