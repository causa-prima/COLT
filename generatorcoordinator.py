from multiprocessing import Event, Lock, Process, Queue, Value

from datagenerator import DataGenerator, WorkloadGenerator, QueryGenerator, LogGenerator


class GeneratorCoordinator(object):

    def __init__(self, max_generated=0, queue_target_size=100000, max_processes=50):
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
        # TODO: needs to be managed per table!
        self.max_generated = Value('L', max_generated)

        # target sizes of queues
        self.queue_target_size = queue_target_size
        # size at which the coordinator should be notified if queue
        # has less items than this.
        self.queue_notify_size = .5 * self.queue_target_size

        # Event for the coordinator to check if new processes have to
        # be created
        self.needs_supervision = Event()
        # Event to tell all child processes to shut down
        self.shutdown = Event()

        # All the queues needed for inter process communication
        self.queue_next_workload = Queue()
        self.queue_workload_data = Queue()
        self.queue_executed_queries = Queue()

        # list of all running processes
        self.processes = []
        # maximum number of processes
        self.max_processes = max_processes

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
        self.shutdown.wait(5)

        self.supervise()

    def generate_generator(self, type):
        if type == 'workload':
            return WorkloadGenerator(out_queue=self.queue_next_workload,
                                     max_generated=self.max_generated,
                                     needs_supervision=self.needs_supervision, #needed?
                                     shutdown=self.shutdown,
                                     queue_target_size=self.queue_target_size,
                                     queue_notify_size=self.queue_notify_size)
        if type == 'data':
            return DataGenerator(in_queue=self.queue_next_workload,
                                 out_queue=self.queue_next_workload,
                                 needs_supervision=self.needs_supervision,
                                 shutdown=self.shutdown,
                                 queue_target_size=self.queue_target_size,
                                 queue_notify_size=self.queue_notify_size)
        if type == 'query':
            return QueryGenerator(in_queue=self.queue_next_workload,
                                  needs_supervision=self.needs_supervision,
                                  shutdown=self.shutdown,
                                  queue_target_size=self.queue_target_size,
                                  queue_notify_size=self.queue_notify_size)
        if type == 'logger':
            return LogGenerator(#TODO: input and/or output queues needed?
                                needs_supervision=self.needs_supervision,
                                shutdown=self.shutdown,
                                queue_target_size=self.queue_target_size,
                                queue_notify_size=self.queue_notify_size)

    def supervise(self):
        while True:
            # wait until something has do be done, but at most 5[1] seconds
            #
            # [1] chosen by fair dice roll
            if self.needs_supervision.wait(5) or self.shutdown.wait(5):

                # if it was decided it is time to shut down - do so!
                if self.shutdown.is_set():
                    # TODO: wait for child processes to exit?
                    break

                new_processes = []
                # check which queue needs more input and create the
                # corresponding generator
                if self.queue_next_workload.qsize() < self.queue_notify_size:
                    new_processes.append(self.generate_generator('workload'))
                if self.queue_workload_data.qsize() < self.queue_notify_size:
                    new_processes.append(self.generate_generator('data'))
                if self.queue_executed_queries.qsize() < self.queue_notify_size:
                    new_processes.append(self.generate_generator('query'))

                # check if the logger needs more processes
                if self.queue_executed_queries.qsize() > .9 * self.queue_target_size:
                    new_processes.append(self.generate_generator('logger'))

                # start all newly generated processes
                for proc in new_processes:
                    self.processes.append(proc)
                    proc.start()

                # reset the signal
                self.needs_supervision.clear()

                # now wait for a second, but wake up if receiving the
                # shutdown signal
                self.shutdown.wait(1)
        # Print the number of generated data items
        print 'max_generated:', self.max_generated
        # TODO: is there more that needs to be done before shutting down?