from multiprocessing import Process
from time import time
from datetime import datetime


class BaseGenerator(Process):
    """Prototype for all generators. It has queues for data in- and
    output, events to notify a need for supervision and to signal a
    wanted shutdown, and parameters for the queue target size and the
    queue size limit for the signal raising.

    """
    def __init__(self, queue_in=None, queue_out=None,
                 queue_target_size=0, queue_notify_size=0,
                 needs_more_input=None, shutdown=None,
                 config=None):

        Process.__init__(self)

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
        # TODO: pass only needed information
        self.config = config

    def after_init(self):
        """ Method called once between the process creation and process running
        to construct class-owned objects that need creation.
        """
        pass

    def run(self):
        self.after_init()
        self._run()

    def _run(self):
        # TODO: docstring
        while True:
            # Check whether there is already enough data in the
            # output queue, and wait while there is.
            while (self.queue_out is not None) and\
                    (self.queue_out.qsize() > self.queue_target_size) and\
                    not self.shutdown.is_set():
                # wait for more input, but don't ignore the shutdown signal
                # TODO: How long should be waited?
                self.shutdown.wait(.00001)

            # if it was decided it is time to shut down - do so!
            if self.shutdown.is_set():
                break

            # check whether more input is needed
            if (self.queue_in is not None) and\
                    (self.queue_in.qsize() < self.queue_notify_size):
                self.needs_more_input.set()

            # there is something to do, so let's go!
            self.process_item()

    def process_item(self):
    # TODO: docstring
        raise NotImplementedError


class WorkloadGenerator(BaseGenerator):
    # TODO: DocString
    # TODO: the WorkloadGenerator is too Cassandra-specific, either solve that or put it into a own module
    generator = None
    def __init__(self, queue_in=None, queue_out=None,
                 queue_target_size=0, queue_notify_size=0,
                 needs_more_input=None, shutdown=None,
                 config=None, key_structs=None, generator_class=None):

        self.generator_class = generator_class

        BaseGenerator.__init__(self, queue_in=queue_in, queue_out=queue_out,
                           queue_target_size=queue_target_size,
                           queue_notify_size=queue_notify_size,
                           needs_more_input=needs_more_input,
                           shutdown=shutdown, config=config)

        self.key_structs = key_structs

        # aggregate the chances and map the chances
        # of each workload into [0,ratio_sum]
        self.ratio_sum = 0
        self.ratio_nums = {}
        for workload_name, workload_data in config['workloads'].items():
            self.ratio_nums[self.ratio_sum] = workload_name
            self.ratio_sum += workload_data['ratio']

    def after_init(self):
        self.generator = self.generator_class()

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
            query_data = []
            # queries without attributes don't need seeds
            if len(query['attributes']) == 0:
                queries.append((False, query_data))
                continue
            # we need the bitmap of seeds that were used as primary keys
            table = query['table']
            key_struct = self.key_structs[table]
            # check if a new data item might be generated
            if query['type'] == 'insert':
                # lock the bitmap for the table the item will be stored in
                with key_struct.lock:
                    partition_seed = key_struct.length()
                    cluster_seed = partition_seed
                    # seed the generator to produce regeneratable results
                    self.generator.seed(partition_seed)
                    # determine if this seed just generates
                    # a new cluster for an old primary key
                    new_cluster = query['chance'] >= self.generator.random()
                    if new_cluster and partition_seed > 0:
                        # We need an old key that created a completely new
                        # item, so we randomly iterate over old keys until we
                        # find one that did. If no key was ever used to create
                        # a completely new item, we fall back to seed zero,
                        # which always generates a completely new item.
                        while True:
                            cluster_seed = self.generator.randrange(0, partition_seed)
                            if cluster_seed == 0 or key_struct[cluster_seed]:
                                break

                    # Tell the other processes what happened by appending the
                    # choice we made to the bitmap. The or-part makes sure that
                    # bitmap[0] is always 1, i.e. seed zero always generates a
                    # completely new item
                    key_struct.append(not(new_cluster) or cluster_seed == partition_seed)

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
                with key_struct.lock:
                    # determine the highest used seed
                    # Notice that the data item produced by this seed might not
                    # be in the database if an error occurred while processing.
                    # See comment on max_inserted LogGenerator for more details.
                    ks_length = key_struct.length()
                    if ks_length > 1:
                        partition_seed = self.generator.randrange(0, ks_length)
                    else:
                        partition_seed = 0
                    cluster_seed = partition_seed
                    # if this seed did not produce a completely new item the
                    # partition key it did produce a new cluster for is needed
                    if not (partition_seed >= ks_length) and\
                            not key_struct[partition_seed]:
                        self.generator.seed(partition_seed)
                        # When generating a new cluster it is first tested if
                        # a new cluster will be generated by using a random
                        # number, so we need to advance the generator one step.
                        self.generator.random()
                        # now we can search for the partition key that was used
                        # notice this takes 1/chance steps on average
                        while True:
                            cluster_seed = self.generator.randrange(0, partition_seed+1)
                            if key_struct[cluster_seed] or cluster_seed == 0:
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
                data = (attribute['type'], seed, attribute['generator args'])
                # append the attribute data to the list of attributes for that
                # query
                query_data.append(data)
            # append a marker for new objects and the data for this query to
            # the list of queries for that workload
            queries.append((query['type'] == 'insert', query_data))

        # put the workload with its data into the queue
        self.queue_out.put((workload_name, queries))


class DataGenerator(BaseGenerator):
    # TODO: DocString

    generator = None

    def __init__(self, queue_in=None, queue_out=None,
                 queue_target_size=0, queue_notify_size=0,
                 needs_more_input=None, shutdown=None,
                 config=None,
                 generator_class=None):

        self.generator_class = generator_class

        BaseGenerator.__init__(self, queue_in=queue_in, queue_out=queue_out,
                           queue_target_size=queue_target_size,
                           queue_notify_size=queue_notify_size,
                           needs_more_input=needs_more_input,
                           shutdown=shutdown, config=config)

    def after_init(self):
        self.generator = self.generator_class()

    def process_item(self):
        """ Generates the data for workload from the input queue and
        puts the result into the output queue.
        """

        # get and unpack the item we want to process
        workload_name, queries = self.queue_in.get()

        # Each workload could have multiple queries. Each query could need
        # multiple columns. Each column could be needed more than once. Each
        # column instance could be needed with different configurations.
        workload_data = []
        for new, query in queries:
            query_values = []
            for type, seed, generator_args in query:

                # reseed the generator to generate the wanted item
                self.generator.seed(seed)
                try:
                    val = self.generator.methods_switch[type](**generator_args)
                except KeyError:
                    msg = "generator for type %s not implemented!" % type
                    raise NotImplementedError(msg)
                query_values.append(val)

            # append the data for that query to the workload data
            workload_data.append((new, query_values))

        # repack the item and put it into the output queue
        self.queue_out.put((workload_name, workload_data))


class QueryGenerator(BaseGenerator):
    # TODO: DocString

    connection = None

    def __init__(self, queue_in=None, queue_out=None,
                 queue_target_size=0, queue_notify_size=0,
                 needs_more_input=None, shutdown=None,
                 config=None,
                 connection_class=None, connection_args=None):

        self.connection_class = connection_class
        self.connection_args = connection_args

        BaseGenerator.__init__(self, queue_in=queue_in, queue_out=queue_out,
                           queue_target_size=queue_target_size,
                           queue_notify_size=queue_notify_size,
                           needs_more_input=needs_more_input,
                           shutdown=shutdown, config=config)

    def after_init(self):
        self.connection = self.connection_class(**self.connection_args)

    def process_item(self):
        """ Generates queries with data from the input queue,
        submits queries to the DB and puts a object that will
        eventually receive the result into the output queue.
        """

        # get and unpack the item we want to process
        workload_name, workload_data = self.queue_in.get()

        query_num = 0
        queries = self.config['workloads'][workload_name]['queries']
        for new, query_values in workload_data:
            # for each query, get the prepared statement and call the connection
            # object to bind and execute the query, which automatically puts
            # resulting execution times into the out_queue
            prep_stmnt = queries[query_num]['prepared_statement']
            self.connection.execute(prep_stmnt, query_values, self.queue_out,
                                    metadata=(workload_name, query_num, new))

            query_num += 1


class LogGenerator(BaseGenerator):
    # TODO: DocString
    def __init__(self, queue_in=None, queue_out=None,
                 queue_target_size=0, queue_notify_size=0,
                 needs_more_input=None, shutdown=None,
                 config=None,
                 max_inserted=None, latencies=None,
                 queue_max_time=None, needs_more_processes=None):

        BaseGenerator.__init__(self, queue_in=queue_in, queue_out=queue_out,
                           queue_target_size=queue_target_size,
                           queue_notify_size=queue_notify_size,
                           needs_more_input=needs_more_input,
                           shutdown=shutdown, config=config)

        self.max_inserted = max_inserted
        # dict to log the execution times as datetime.timedelta
        self.latencies = latencies
        self.queue_max_time = queue_max_time
        self.needs_more_processes = needs_more_processes

        self.now = int(time())
        self.processed_latencies = []

    def process_item(self):
        result, start, end, (workload_name, query_num, new) = self.queue_in.get()
        now = time()
        time_in_queue = (datetime.fromtimestamp(now) - end).total_seconds()

        # check whether more LogGenerator processes are needed
        if time_in_queue > self.queue_max_time or\
                        self.queue_in.qsize() > self.queue_target_size:
            self.needs_more_processes.set()
            # print 'time in queue:', time_in_queue

        now = int(now)
        num_queries = len(self.processed_latencies)
        # check whether the next second is reached and if there is output data
        if (now > self.now) and num_queries > 0:
            # put the results into the GeneratorCoordinator's
            # synchronized objects and reset the variables
            # TODO: synchronization does NOT work!
            with self.latencies.lock:
                try:
                    # print '%5i queries reported by %s' % (num_queries, self.name)
                    latencies = self.latencies[self.now]
                    latencies.extend(self.processed_latencies)
                    self.latencies[self.now] = latencies
                except KeyError:
                    self.latencies[self.now] = self.processed_latencies
            self.now = now
            self.processed_latencies = []

        # do not log execution times of errors
        # TODO: test error case
        if result is None:
                self.processed_latencies.append((end - start, workload_name, query_num))


        # Increment the max_inserted counter if a new item was generated.
        # Note that this number does not necessarily represent the seed that
        # was used to generate the new item, as queries should happen
        # asynchronously. To guarantee that max_inserted equals the maximum
        # seed related to an item in the database, another way of storing this
        # information would be needed. This would introduce more complexity and
        # overhead. The chosen approach should work well enough as long as no
        # errors occur when inserting new data, hence this case should be
        # considered.
        if new:
            if result is not None:
                msg = 'New item should have been inserted, but an error occured.'
                raise Warning(msg)
            table = self.config['workloads'][workload_name]['queries'][query_num]['table']
            with self.max_inserted[table].get_lock():
                self.max_inserted[table].value += 1