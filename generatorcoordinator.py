from multiprocessing import Event, Lock, Process
from multiprocessing.managers import SyncManager, MakeProxyType
from time import time, sleep
from datetime import datetime

from bitarray import bitarray
from pyjudy import JudyLIntInt

from datagenerator import DataGenerator, WorkloadGenerator, QueryGenerator, LogGenerator

class GeneratorCoordinator(object):
    # Create a proxys for non-standard types so all their methods can be used.
    # First, define all methods that should not be exposed, than substract
    # them from all methods offered by that type. Finally, make a proxy type
    # with all wanted methods.
    non_exposed = set(("__class__", "__copy__", "__deepcopy__", "__delattr__",
                       "__dict__", "__doc__", "__format__", "__getattribute__",
                       "__init__", "__module__", "__new__", "__repr__",
                       "__setattr__", "__str__", "__subclasshook__"))

    bitarray_methods = tuple(set(dir(bitarray)) - non_exposed)
    bitarrayProxy = MakeProxyType('bitarrayProxy', bitarray_methods)

    JudyLIntInt_methods = tuple(set(dir(JudyLIntInt)) - non_exposed)
    JudyLIntIntProxy = MakeProxyType('JudyLIntIntProxy', JudyLIntInt_methods)


    # register the types to use them as a managed object
    SyncManager.register('bitarray', bitarray, bitarrayProxy)
    SyncManager.register('JudyLIntInt', JudyLIntInt, JudyLIntIntProxy)

    manager = SyncManager()
    manager.start()

    def __init__(self, config, random_class, connection_class, connection_args={},
                 queue_target_size=100, max_processes=4):
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
        # TODO: correct docstring
        # The number of generated data items within each table.
        # If there was already data generated with by same rules one
        # can state this in the config file for each table.
        # The values are stored as a unsigned long Value() with
        # integrated locking mechanism. Maximum is (2^64)-1.
        # Because of the separation of data generation and querying
        # two dicts are needed to handle both processes separately.
        self.key_structs = {}
        for table in config['tables'].keys():
            self.key_structs[table] = {}

            bitmap = self.manager.bitarray()
            bitmap.lock = Lock()
            self.key_structs[table]['bitmap'] = bitmap

            update_dict = self.manager.JudyLIntInt()
            update_dict.lock = Lock()
            self.key_structs[table]['update_dict'] = update_dict
            # TODO: import old data: import key bitmap or regenerate?

        # target sizes of queues
        self.queue_target_size = queue_target_size
        # size at which the coordinator should be notified if queue
        # has less items than this.
        self.queue_notify_size = .5 * self.queue_target_size

        # All the queues needed for inter process communication
        self.queues = {'next_workload': self.manager.Queue(),
                       'workload_data': self.manager.Queue(),
                       'executed_queries': self.manager.Queue()}

        # Events for the coordinator to check if new processes have to be
        # created. Each class of generators gets it own Event.
        self.events = {'DataGenerators': Event(), 'QueryGenerators': Event(),
                       'LogGenerators': Event(), 'LogGenerators2': Event(),
                       'shutdown': Event()}

        # Event to tell all child processes to shut down

        # convenience event to wait for all events simultaneously
        self.supervision_needed = OrEvent(*self.events.values())

        # Log data of execution times
        self.latencies = self.manager.dict()
        self.latencies.lock = self.manager.Lock()

        # list of all running processes
        self.processes = []
        # maximum number of processes
        self.max_processes = max_processes

        self.random_class = random_class
        self.connection_class = connection_class
        self.connection_args = connection_args
        # TODO: share one connection with all processes if possible (see http://www.datastax.com/dev/blog/4-simple-rules-when-using-the-datastax-drivers-for-cassandra for details why)
        self.connection = connection_class(**connection_args)

        self.config = config

    def start(self):
        wl_generator = self.create_generator('Workload')
        data_generator = self.create_generator('Data')
        query_generator = self.create_generator('Query')
        logger = self.create_generator('Log')
        watcher = Process(target=watch_and_report,
                          args=(self.config, self.latencies, self.events))
        self.processes = [wl_generator, data_generator,
                          query_generator, logger,
                          watcher]
        for process in self.processes:
            process.start()

        # wait for some time for the queues to fill before starting
        # to supervise, but don't ignore the shutdown signal
        self.events['shutdown'].wait(5)

        self.supervise()

    def create_generator(self, generator_type):
        print 'creating new %sGenerator' % generator_type
        if generator_type == 'Workload':
            return WorkloadGenerator(queue_out=self.queues['next_workload'],
                                     shutdown=self.events['shutdown'],
                                     queue_target_size=self.queue_target_size,
                                     queue_notify_size=self.queue_notify_size,
                                     config=self.config,
                                     key_structs=self.key_structs,
                                     generator_class=self.random_class)
        if generator_type == 'Data':
            return DataGenerator(queue_in=self.queues['next_workload'],
                                 queue_out=self.queues['workload_data'],
                                 needs_more_input=self.events['DataGenerators'],
                                 shutdown=self.events['shutdown'],
                                 queue_target_size=self.queue_target_size,
                                 queue_notify_size=self.queue_notify_size,
                                 config=self.config,
                                 generator_class=self.random_class)
        if generator_type == 'Query':
            return QueryGenerator(queue_in=self.queues['workload_data'],
                                  queue_out=self.queues['executed_queries'],
                                  needs_more_input=self.events['QueryGenerators'],
                                  shutdown=self.events['shutdown'],
                                  queue_target_size=self.queue_target_size,
                                  queue_notify_size=self.queue_notify_size,
                                  config=self.config,
                                  connection_class=self.connection_class,
                                  connection_args=self.connection_args)
        if generator_type == 'Log':
            return LogGenerator(queue_in=self.queues['executed_queries'],
                                needs_more_input=self.events['LogGenerators'],
                                shutdown=self.events['shutdown'],
                                queue_target_size=self.queue_target_size,
                                queue_notify_size=self.queue_notify_size,
                                config=self.config,
                                # time an item is allowed to be queued
                                # TODO: set this to value of connection/query timeout
                                queue_max_time=10,
                                latencies=self.latencies,
                                needs_more_processes=self.events['LogGenerators2'])

    def supervise(self):
        events = self.events
        queues = self.queues
        notify_size = self.queue_notify_size
        target_size = self.queue_target_size
        while True:
            # wait until something has do be done
            self.supervision_needed.wait()

            # if it was decided it is time to shut down - do so!
            if events['shutdown'].is_set():
                # wait for all child processes to end before leaving the
                # while-loop
                while len(self.processes) > 0:
                    proc = self.processes.pop(0)
                    proc.join(.1)
                    if proc.is_alive():
                        self.processes.append(proc)
                break

            new_processes = []

            if len(self.processes) >= self.max_processes:
                continue

            # check which queue needs more input and create the
            # corresponding generator if needed
            # TODO: Signal raising only considers the preceding generator class.
            # This generator class might also not have enough input data, so it
            # might be senseless to create a new process for that class.
            if events['DataGenerators'].is_set() and \
                        queues['next_workload'].qsize() < notify_size:
                new_processes.append(self.create_generator('Workload'))
            if events['QueryGenerators'].is_set() and \
                        queues['workload_data'].qsize() < notify_size:
                new_processes.append(self.create_generator('Data'))
            if events['LogGenerators'] and \
                        queues['executed_queries'].qsize() < notify_size:
                new_processes.append(self.create_generator('Query'))

            # check if more LogGenerators are needed
            if events['LogGenerators2'] or \
                        queues['executed_queries'].qsize() > target_size:
                new_processes.append(self.create_generator('Log'))

            # start all newly generated processes
            for proc in new_processes:
                if len(self.processes) >= self.max_processes:
                    print 'Maximum number of processes reached.'
                    break
                self.processes.append(proc)
                proc.start()

            # reset all signals
            for event in events.values():
                event.clear()

            # now wait for a second, but wake up if receiving the
            # shutdown signal
            events['shutdown'].wait(1)
        # TODO: what needs to be done before shutting down?


def watch_and_report(config, logs, events):
    # TODO: docstring
    # TODO: check for arbitrary termination conditions?
    term_conds = config['config']['termination conditions']
    max_latency = term_conds['latency']['max']
    consec_latencies = term_conds['latency']['consecutive']
    try:
        max_queries = term_conds['queries']['max']
    except KeyError:
        max_queries = None
    consec_queries = term_conds['queries']['consecutive']
    succ_latencies = 0
    succ_queries = 0
    last_num_queries = 0
    queries_sum = 0
    latency_sum_o = 0
    tests=0

    while True:
        last_second = int(time())-1
        timepoint = datetime.fromtimestamp(last_second)
        try:
            # TODO: output a chosen set of percentiles instead of the mean
            # print only values of the last second, as the older ones
            # don't change anymore
            tests += 1
            latencies = logs[last_second]
            queries = len(latencies)
            queries_sum += queries
            latency_sum = 0

            for latency, _, _ in latencies:
                latency_sum += latency.total_seconds()*1000
            latency_sum_o += latency_sum

            latency = latency_sum/queries

        except KeyError:
            print timepoint, 'No data'
            # sleep until the next second, then continue
            sleep(last_second+2.25-time())
            continue
        msg = '%s     queries/sec: %10i     avg latency (last secons): %10.2f ms     ' \
              'queries/sec avg: %10i     latency avg (runtime): %10.2f ms'
        print msg % (timepoint, queries, latency, queries_sum/tests, latency_sum_o/queries_sum)

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
        max_queries_b = (max_queries is not None) and (queries > max_queries)
        msgs = []
        if latencies:
            msg = '%s Latency was over %s ms for %s consecutive seconds'
            msg = msg % (timepoint, max_latency, succ_latencies)
            msgs.append(msg)
        if num_queries:
            msg = '%s Number of queries has fallen %s consecutive seconds'
            msg = msg % (timepoint, succ_queries)
            msgs.append(msg)
        if max_queries_b:
            msg = '%s Number of queries per second has risen over %s'
            msg = msg % (timepoint, max_queries)
            msgs.append(msg)

        if latencies or num_queries or max_queries_b:
            msgs.append('%s Shutting down.' % timepoint)
            print '\n'.join(msgs)
            events['shutdown'].set()
            break

        # sleep until the next second
        sleep(max(last_second+2.25-time(),0))


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