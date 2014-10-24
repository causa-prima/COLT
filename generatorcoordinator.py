from multiprocessing import Event, Lock, Process, Queue, Value

from bitarray import bitarray

from datagenerator import DataGenerator, WorkloadGenerator, QueryGenerator, LogGenerator


class GeneratorCoordinator(object):

    def __init__(self, config, queue_target_size=100000, max_processes=50):
        # The GeneratorCoordinator spawns multiple processes for data
        # and query generation. Generators communicate via queues,
        # each generator shares a queue for input and/or output with
        # other generators. In case the input queue gets to small
        # each generator can emit a signal to the coordinator, which
        # then (under certain conditions) spawns a new process to
        # fill that queue.
        # Workload generators are supposed to "report" the generation
        # of new data via incrementing the max_generated_seed.

        # The number of generated data items within each table.
        # If there was already data generated with by same rules one
        # can state this in the config file for each table.
        # The values are stored as a unsigned long Value() with
        # integrated locking mechanism. Maximum is (2^64)-1.
        # Because of the separation of data generation and querying
        # two dicts are needed to handle both processes separately.
        self.key_structs = {}
        for keyspace in config['keyspaces'].keys():
            self.key_structs[keyspace] = {}
            for table in config['keyspaces'][keyspace].keys():
                key_struct = object()
                key_struct.lock = Lock()
                key_struct.bitmap = bitarray()
                self.key_structs[keyspace][table] = key_struct
                # TODO: import old key bitmap - or regenerate?

        # TODO: numbers here can be incorrect on runtime
        # See LogGenerator's process_item for further information.
        self.max_inserted = {}
        for keyspace in config['keyspaces'].keys():
            self.max_inserted[keyspace] = {}
            for table in config['keyspaces'][keyspace].keys():
                try:
                    self.max_inserted[keyspace][table] = Value('L', config['keyspaces'][keyspace][table]['max_inserted'])
                except KeyError:
                    self.max_inserted[keyspace][table] = Value('L', 0)

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
        self.supervision_needed = OrEvent(self.events.values())

        # Log data of execution times
        self.logs = object()
        self.logs.lock = Lock()
        self.logs.values = {}

        # list of all running processes
        self.processes = []
        # maximum number of processes
        self.max_processes = max_processes

        self.config = config

    def start(self):
        wl_generator = self.generate_generator('workload')
        data_generator = self.generate_generator('data')
        query_generator = self.generate_generator('query')
        logger = self.generate_generator('logger')

        self.processes = [wl_generator, data_generator, query_generator, logger]
        for process in self.processes:
            process.start()

        # wait for some time for the queues to fill before starting
        # to supervise, but don't ignore the shutdown signal
        self.events['shutdown'].wait(5)

        self.supervise()

    def generate_generator(self, type):
        if type == 'workload':
            return WorkloadGenerator(queue_out=self.queues['next_workload'],
                                     shutdown=self.events['shutdown'],
                                     queue_target_size=self.queue_target_size,
                                     queue_notify_size=self.queue_notify_size,
                                     config=self.config,
                                     key_structs=self.key_structs)
        if type == 'data':
            return DataGenerator(queue_in=self.queues['next_workload'],
                                 queue_out=self.queues['workload_data'],
                                 needs_more_input=self.events['DataGenerators'],
                                 shutdown=self.events['shutdown'],
                                 queue_target_size=self.queue_target_size,
                                 queue_notify_size=self.queue_notify_size,
                                 config=self.config)
        if type == 'query':
            return QueryGenerator(queue_in=self.queues['workload_data'],
                                  queue_out=self.queues['executed_queries'],
                                  needs_more_input=self.events['QueryGenerators'],
                                  shutdown=self.events['shutdown'],
                                  queue_target_size=self.queue_target_size,
                                  queue_notify_size=self.queue_notify_size,
                                  config=self.config) # TODO: hand over a connection object
        if type == 'logger':
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