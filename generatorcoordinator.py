from multiprocessing import Event, Lock, Process, Queue, Value

from workloadgenerator import WorkloadGenerator
from datagenerator import DataGenerator
from querygenerator import QueryGenerator
from logger import Logger


class GeneratorCoordinator(object):

    def __init__(self, max_generated=0):
        # The GeneratorCoordinator spawns multiple processes for data
        # and query generation. Generators communicate via queues,
        # each generator shares a queue for input and/or output with
        # other generators. In case the input queue gets to small
        # each generator can emit a signal to the coordinator, which
        # then (under certain conditions) spawns a new process to
        # fill that queue.
        # Workload generators are supposed to "report" the generation
        # of new data via incrementing the max_generated_seed.

        # The number of generated data items.
        # If there was already data generated with by same rules one
        # can pass this on construction of the GeneratorCoordinator.
        # The value is stored in a unsigned long Value() with
        # integrated locking mechanism. Maximum is (2^64)-1.
        self.max_generated = Value('L', max_generated)

        # target sizes of queues
        self.queue_target_size = 10000
        # size at which the coordinator should be notified if queue
        # has less items than this.
        self.queue_notify_threshold = .5 * self.queue_target_size

        # Event for the coordinator to check if new processes have to
        # be created
        self.needs_action = Event()
        # Event to tell all child processes to shut down
        self.shutdown = Event()

        # All the queues needed for inter process communication
        self.queue_next_workload = Queue()
        self.queue_workload_data = Queue()
        self.queue_executed_queries = Queue()

        # list of all running processes
        self.processes = []

    def start(self):
        wl_generator = self.generate_generator('workload')
        data_generator = self.generate_generator('data')
        query_generator = self.generate_generator('query')
        logger = self.generate_generator('logger')

        self.processes = [wl_generator, data_generator, query_generator, logger]
        for process in self.processes:
            process.start()
        self.supervise()

    def generate_generator(self, type):
        if type == 'workload':
            return WorkloadGenerator(workloads=self.queue_next_workload,
                                         max_generated=self.max_generated,
                                         needs_action=self.needs_action, #needed?
                                         shutdown=self.shutdown,
                                         queue_target_size=self.queue_target_size,
                                         queue_notify_threshold=self.queue_notify_threshold)
        if type == 'data':
            return DataGenerator(workloads=self.queue_next_workload,
                                       wl_data=self.queue_next_workload,
                                         needs_action=self.needs_action,
                                         shutdown=self.shutdown,
                                         queue_target_size=self.queue_target_size,
                                         queue_notify_threshold=self.queue_notify_threshold)
        if type == 'query':
            return QueryGenerator(wl_data=self.queue_next_workload,
                                         needs_action=self.needs_action,
                                         shutdown=self.shutdown,
                                         queue_target_size=self.queue_target_size,
                                         queue_notify_threshold=self.queue_notify_threshold)
        if type == 'logger':
            return Logger(
                                         needs_action=self.needs_action,
                                         shutdown=self.shutdown,
                                         queue_target_size=self.queue_target_size,
                                         queue_notify_threshold=self.queue_notify_threshold)

    def supervise(self):
        while True:
            # wait until something has do be done, but at most 5[1] seconds
            #
            # [1] chosen by fair dice roll
            if self.needs_action.wait(5) or self.shutdown.wait(5):

                # if it was decided it is time to shut down - do so!
                if self.shutdown.is_set():
                    # TODO: wait for child processes to exit?
                    break

                new_processes = []
                # check which queue needs more input and create the
                # corresponding generator
                if self.queue_next_workload.qsize() < self.queue_notify_threshold:
                    new_processes.append(self.generate_generator('workload'))
                if self.queue_workload_data.qsize() < self.queue_notify_threshold:
                    new_processes.append(self.generate_generator('data'))
                if self.queue_executed_queries.qsize() < self.queue_notify_threshold:
                    new_processes.append(self.generate_generator('query'))

                # check if the logger needs more processes
                if self.queue_executed_queries.qsize() > .9 * self.queue_target_size:
                    new_processes.append(self.generate_generator('logger'))

                # start all newly generated processes
                for proc in new_processes:
                    self.processes.append(proc)
                    proc.start()

                # reset the signal
                self.needs_action.clear()

                # now wait for a second, but wake up if receiving the
                # shutdown signal
                self.shutdown.wait(1)