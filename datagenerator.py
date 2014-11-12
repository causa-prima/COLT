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


    def run(self):
        # TODO: docstring
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

    def process_item(self):
    # TODO: docstring
        raise NotImplementedError


class WorkloadGenerator(BaseGenerator):
    # TODO: DocString
    # TODO: the WorkloadGenerator is too Cassandra-specific, either solve that or put it into a own module
    def __init__(self, queue_in=None, queue_out=None,
                 queue_target_size=0, queue_notify_size=0,
                 needs_more_input=None, shutdown=None,
                 config=None, key_structs=None, max_inserted=None,
                 generator=None):
        BaseGenerator.__init__(self, queue_in=queue_in, queue_out=queue_out,
                           queue_target_size=queue_target_size,
                           queue_notify_size=queue_notify_size,
                           needs_more_input=needs_more_input,
                           shutdown=shutdown, config=config)

        self.key_structs = key_structs
        self.max_inserted = max_inserted
        self.generator = generator

        # aggregate the chances and map the chances
        # of each workload into [0,ratio_sum]
        self.ratio_sum = 0
        self.ratio_nums = {}
        for workload_name, workload_data in config['workloads'].items():
            self.ratio_nums[self.ratio_sum] = workload_name
            self.ratio_sum += workload_data['ratio']

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
                with self.max_inserted[table].get_lock():
                    partition_seed = self.max_inserted[table].value

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
                data = (attribute['type'], seed, attribute['generator args'])
                # append the attribute data to the list of attributes for that
                # query
                query_data.append(data)
            # append a marker for new objects and the data for this query to
            # the list of queries for that workload
            queries.append((query['type'] == 'insert', query_data))

        # put the workload with its data into the queue
        self.queue_out.put((workload, queries))


class DataGenerator(BaseGenerator):
    # TODO: DocString
    def __init__(self, queue_in=None, queue_out=None,
                 queue_target_size=0, queue_notify_size=0,
                 needs_more_input=None, shutdown=None,
                 config=None, connection_args_dict={},
                 generator=None):
        BaseGenerator.__init__(self, queue_in=queue_in, queue_out=queue_out,
                           queue_target_size=queue_target_size,
                           queue_notify_size=queue_notify_size,
                           needs_more_input=needs_more_input,
                           shutdown=shutdown, config=config)

        self.generator = generator

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
        self.queue_out.put((workload, workload_data))


class QueryGenerator(BaseGenerator):
    # TODO: DocString

    connection = None

    def __init__(self, queue_in=None, queue_out=None,
                 queue_target_size=0, queue_notify_size=0,
                 needs_more_input=None, shutdown=None,
                 config=None,
                 connection_class=None, connection_args=None):

        BaseGenerator.__init__(self, queue_in=queue_in, queue_out=queue_out,
                           queue_target_size=queue_target_size,
                           queue_notify_size=queue_notify_size,
                           needs_more_input=needs_more_input,
                           shutdown=shutdown, config=config)

        # the connection has to be established _AFTER_ the subprocess was
        # created, so we need to decorate the 'run()' method of BaseGenerator
        self.connection_class = connection_class
        self.connection_args = connection_args
        self._run = super(QueryGenerator, self).run

    def run(self):
        # decorating the 'run()' method of BaseGenerator to establish the
        # connection after subprocess creation
        self.connection = self.connection_class(**self.connection_args)
        self._run()

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


class LogGenerator(BaseGenerator):
    # TODO: DocString
    def __init__(self, queue_in=None, queue_out=None,
                 queue_target_size=0, queue_notify_size=0,
                 needs_more_input=None, shutdown=None,
                 config=None, max_inserted=None, logs=None,
                 queue_max_time=None, needs_more_processes=None):

        BaseGenerator.__init__(self, queue_in=queue_in, queue_out=queue_out,
                           queue_target_size=queue_target_size,
                           queue_notify_size=queue_notify_size,
                           needs_more_input=needs_more_input,
                           shutdown=shutdown, config=config)

        self.max_inserted = max_inserted
        # dict to log the execution times as datetime.timedelta
        self.logs = logs
        self.queue_max_time = queue_max_time
        self.needs_more_processes = needs_more_processes

        self.now = int(time())
        self.latencies = []
        self.num_queries = 0

    def process_item(self):
        result, start, end, (workload, query_num, new) = self.queue_in.get()
        now = time()
        time_in_queue = (datetime.fromtimestamp(now) - end).total_seconds()

        # check whether more LogGenerator processes are needed
        if time_in_queue > self.queue_max_time or\
                        self.queue_in.qsize() > self.queue_target_size:
            self.needs_more_processes.set()
            print 'time in queue:', time_in_queue

        now = int(now)
        # check whether the next second is reached and
        # if there is output for queue_out
        if (now > self.now) and self.num_queries > 0:
            # put the results into the GeneratorCoordinator's
            # synchronized objects and reset the variables
            # TODO: synchronization does NOT work!
            with self.logs.lock:
                try:
                    latencies = self.logs.latencies[self.now]
                    latencies.extend(self.latencies)
                    self.logs.latencies[self.now] = latencies
                    self.num_queries += self.logs.queries[self.now]
                    self.logs.queries[self.now] += self.num_queries
                except KeyError:
                    self.logs.latencies[self.now] = self.latencies
                    self.logs.queries[self.now] = self.num_queries
            self.now = now
            self.latencies = []
            self.num_queries = 0

        # do not log execution times of errors
        # TODO: test error case
        if result is not None:
                self.latencies.append((end - start, workload, query_num))
                self.num_queries += 1


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
            if result:
                msg = 'New item should have been inserted, but an error occured.'
                raise Warning(msg)
            table = workload['queries'][query_num]['table']
            with self.max_inserted[table].get_lock():
                self.max_inserted[table].value += 1