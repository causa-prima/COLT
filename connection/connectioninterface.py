class ConnectionInterface(object):
    """
    Interface for DB-connections.

    Methods for
        * establishing a connection,
        * clearing a connection, and
        * non-blocking, asynchronous query execution
    have to be implemented by subclasses.
    """

    def __init__(self, **connection_args):
        self.connection_args = connection_args
        self.connect()

    def __del__(self):
        self.shutdown()

    def connect(self):
        raise NotImplementedError

    def shutdown(self):
        raise NotImplementedError

    def reset(self):
        self.shutdown()
        self.connect()

    def execute(self, query, parameters, out_queue, metadata=None):
        """ Binds parameters to a given query and executes it non-blocking and
        asynchronously, putting an object interpretable by the LogGenerator
        into out_queue

        :param query: the query to execute
        :param parameters: the parameters to bind to the query before execution
        :param out_queue: queue used by the LogGenerators to log events
        :param metadata: metadata about the executed query. default = None
        """
        raise NotImplementedError
