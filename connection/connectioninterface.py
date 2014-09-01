class ConnectionInterface(object):
    """
    Interface for DB-connections.

    Methods for
        * establishing a connection,
        * clearing a connection, and
        * non-blocking, asynchronous query execution
    have to be implemented by subclasses.
    """

    def __init__(self, **kwargs):
        self.connect(**kwargs)

    def __del__(self):
        self.shutdown()

    def connect(self, **kwargs):
        raise NotImplementedError

    def shutdown(self):
        raise NotImplementedError

    def reset(self):
        self.shutdown()
        self.connect()

    def execute(self, query):
        """ Executes a given query non-blocking and asynchronously,
        using logger.log_result and logger.log_error for responses.
        """
        raise NotImplementedError