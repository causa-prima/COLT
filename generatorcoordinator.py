from multiprocessing import Event, Lock, Process, Queue, Value
from time import time, sleep
from datetime import datetime

from bitarray import bitarray

from datagenerator import DataGenerator, WorkloadGenerator, QueryGenerator, LogGenerator


class GeneratorCoordinator(object):

    def __init__(self, config, random_class, connection_class,
                 queue_target_size=100000, max_processes=50):
        # TODO: complete docstring
        """ The GeneratorCoordinator spawns multiple processes for data
        and query generation. Generators communicate via queues,
        each generator shares a queue for input and/or output with
        other generators ot the same type. In case the input queue gets to
        small each generator can emit a signal to the coordinator, which
        then (under certain conditions) spawns a new process filling that queue.
        Workload generators are supposed to "report" the generation
        of new data via adding information to the seed bitmap vector, new data
        items written to the database should increment the max_generated
        structure.

        :param config:
        :param queue_target_size:
        :param max_processes:
        :param random_class:
        :param connection_class:
        :return:
        """

        # The number of generated data items within each table.
        # If there was already data generated with by same rules one
        # can state this in the config file for each table.
        # The values are stored as a unsigned long Value() with
        # integrated locking mechanism. Maximum is (2^64)-1.
        # Because of the separation of data generation and querying
        # two dicts are needed to handle both processes separately.
        self.key_structs = {}
        # The number of generated data items per table written to the database
        # TODO: numbers can be incorrect on runtime
        # See LogGenerator's process_item for further information.
        self.max_inserted = {}
        for table in config['tables'].keys():
            # hacky way to construct an object to
            # which attributes can be assigned
            key_struct = type('', (object,), {})
            key_struct.lock = Lock()
            key_struct.bitmap = bitarray()
            self.key_structs[table] = key_struct
            self.max_inserted[table] = Value('L', 0)
            # TODO: import old data: import key bitmap or regenerate?

        # target sizes of queues
        self.queue_target_size = queue_target_size
        # size at which the coordinator should be notified if queue
        # has less items than this.
        self.queue_notify_size = .5 * self.queue_target_size

        # All the queues needed for inter process communication
        self.queues = {'next_workload': Queue(),
                       'workload_data': Queue(),
                       'executed_queries': Queue()}

        # Events for the coordinator to check if new processes have to be
        # created. Each class of generators gets it own Event.
        self.events = {'DataGenerators': Event(), 'QueryGenerators': Event(),
                       'LogGenerators': Event(), 'LogGenerators2': Event(),
                       'shutdown': Event()}

        # Event to tell all child processes to shut down

        # convenience event to wait for all events simultaneously
        self.supervision_needed = OrEvent(*self.events.values())

        # Log data of execution times
        self.logs = type('', (object,), {})
        self.logs.lock = Lock()
        self.logs.latencies = {}
        self.logs.queries = {}

        # list of all running processes
        self.processes = []
        # maximum number of processes
        self.max_processes = max_processes

        self.random_class = random_class
        self.connection_class = connection_class
        self.connection = connection_class()

        self.config = config

    def start(self):
        wl_generator = self.generate_generator('workload')
        data_generator = self.generate_generator('data')
        query_generator = self.generate_generator('query')
        logger = self.generate_generator('logger')
        watcher = Process(target=self.watch_and_report)
        self.processes = [wl_generator, data_generator,
                          query_generator, logger,
                          watcher]
        for process in self.processes:
            process.start()

        # wait for some time for the queues to fill before starting
        # to supervise, but don't ignore the shutdown signal
        self.events['shutdown'].wait(5)

        self.supervise()

    def generate_generator(self, generator_type):
        if generator_type == 'workload':
            return WorkloadGenerator(queue_out=self.queues['next_workload'],
                                     shutdown=self.events['shutdown'],
                                     queue_target_size=self.queue_target_size,
                                     queue_notify_size=self.queue_notify_size,
                                     config=self.config,
                                     key_structs=self.key_structs,
                                     generator=self.random_class())
        if generator_type == 'data':
            return DataGenerator(queue_in=self.queues['next_workload'],
                                 queue_out=self.queues['workload_data'],
                                 needs_more_input=self.events['DataGenerators'],
                                 shutdown=self.events['shutdown'],
                                 queue_target_size=self.queue_target_size,
                                 queue_notify_size=self.queue_notify_size,
                                 config=self.config,
                                 generator=self.random_class())
        if generator_type == 'query':
            return QueryGenerator(queue_in=self.queues['workload_data'],
                                  queue_out=self.queues['executed_queries'],
                                  needs_more_input=self.events['QueryGenerators'],
                                  shutdown=self.events['shutdown'],
                                  queue_target_size=self.queue_target_size,
                                  queue_notify_size=self.queue_notify_size,
                                  config=self.config,
                                  connection=self.connection_class())
        if generator_type == 'logger':
            return LogGenerator(queue_in=self.queues['executed_queries'],
                                needs_more_input=self.events['LogGenerators'],
                                shutdown=self.events['shutdown'],
                                queue_target_size=self.queue_target_size,
                                queue_notify_size=self.queue_notify_size,
                                config=self.config,
                                # time an item is allowed to be queued
                                # TODO: set it to value of connection/query timeout
                                queue_max_time=10,
                                max_inserted=self.max_inserted,
                                logs=self.logs,
                                needs_more_processes=self.events['LogGenerators2'])

    def supervise(self):
        events = self.events
        queues = self.queues
        notify_size = self.queue_notify_size
        target_size = self.queue_target_size
        while True:
            # wait until something has do be done
            self.supervision_needed.wait()

            # check if any process set an event
            if self.supervision_needed.is_set():

                # if it was decided it is time to shut down - do so!
                if events['shutdown'].is_set():
                    # wait for all child processes to end before leaving the
                    # while-loop
                    for proc in self.processes:
                        proc.join()
                    break

                new_processes = []

                # check which queue needs more input and create the
                # corresponding generator if needed
                if events['DataGenerators'].is_set() and \
                            queues['next_workload'].qsize() < notify_size:
                    new_processes.append(self.generate_generator('workload'))
                if events['QueryGenerators'].is_set() and \
                            queues['workload_data'].qsize() < notify_size:
                    new_processes.append(self.generate_generator('data'))
                if events['LogGenerators'] and \
                            queues['executed_queries'].qsize() < notify_size:
                    new_processes.append(self.generate_generator('query'))

                # check if the logger needs more processes
                if events['LogGenerators2'] or \
                            queues['executed_queries'].qsize() > target_size:
                    new_processes.append(self.generate_generator('logger'))

                # start all newly generated processes
                for proc in new_processes:
                    self.processes.append(proc)
                    proc.start()

                # reset all signals
                for event in events.values():
                    event.clear()

                # now wait for a second, but wake up if receiving the
                # shutdown signal
                events['shutdown'].wait(1)
        # Print the number of generated data items
        print 'max_inserted:', self.max_inserted
        # TODO: what needs to be done before shutting down?

    def watch_and_report(self):
        # TODO: check for arbitrary termination conditions?
        term_conds = self.config['config']['termination conditions']
        max_latency = term_conds['latency']['max']
        consec_latencies = term_conds['latency']['consecutive']
        max_queries = term_conds['queries']['max']
        consec_queries = term_conds['queries']['consecutive']
        succ_latencies = 0
        succ_queries = 0
        last_num_queries = 0

        while True:
            last_second = int(time())-1
            timepoint = datetime.fromtimestamp(last_second)
            try:
                with self.logs.lock:
                    # print only values of the last second, as the older ones
                    # don't change anymore
                    latency = self.logs.latencies[last_second]
                    queries = self.logs.queries[last_second]
            except KeyError:
                print timepoint, 'No data'
                # sleep until the next second, then continue
                sleep(last_second+2.25-time())
                continue
            msg = 'queries/sec: %10i     avg latency: %10.2f ms'
            print timepoint, msg % (queries, latency/queries)

            # if the latency exceeds the defined threshold, increment the value
            # of successive latencies over the threshold
            if latency > max_latency:
                succ_latencies += 1
            else:
                # reset the counter if the latency was below the threshold
                succ_latencies = 0

            # if the number of executed queries falls below the number of the
            # last second increment the value of successive decreasing #queries
            if queries < last_num_queries:
                succ_queries += 1
            else:
                # reset the counter if the #queries was bigger than last second
                succ_queries = 0

            # check if shutdown conditions are met
            # and set shutdown signal accordingly
            latencies = succ_latencies > consec_latencies
            num_queries = succ_queries > consec_queries
            msgs = []
            if latencies:
                msg = '%s Latency was over %s ms for %s consecutive seconds'
                msg = msg % timepoint
                msgs.append(msg % (max_latency, consec_latencies))
            if num_queries:
                msg = '%s Number of queries has fallen %s consecutive seconds'
                msg = msg % timepoint
                msgs.append(msg % consec_queries)
            if queries > max_queries:
                msg = '%s Number of queries per second has risen over %s'
                msg = msg % timepoint
                msgs.append(msg % max_queries)

            if latencies or num_queries or queries > max_queries:
                msgs.append('%s Shutting down.' % timepoint)
                print '\n'.join(msgs)
                self.events['shutdown'].set()
                break

            # sleep until the next second
            sleep(last_second+2.25-time())



# helper functions to provide waiting for multiple events simultaneously,
# found at https://stackoverflow.com/a/12320352/1065901
def or_set(self):
    self._set()
    self.changed()


def or_clear(self):
    self._clear()
    self.changed()


def orify(e, changed_callback):
    e._set = e.set
    e._clear = e.clear
    e.changed = changed_callback
    e.set = lambda: or_set(e)
    e.clear = lambda: or_clear(e)


def OrEvent(*events):
    or_event = Event()
    def changed():
        bools = [e.is_set() for e in events]
        if any(bools):
            or_event.set()
        else:
            or_event.clear()
    for e in events:
        orify(e, changed)
    changed()
    return or_event