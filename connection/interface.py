class Interface(object):
    """
    Interface for DB-connections.

    Methods for
        * establishing a connection,
        * clearing a connection, and
        * non-blocking, asynchronous query execution
    have to be implemented by subclasses.
    """

    def __init__(self):
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

    def execute(self, query):
        """ Executes a given query non-blocking and asynchronously,
        returning a analyze object for response receiving.

        :return: analyze object
        :rtype: analyze
        """
        raise NotImplementedError