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

    def execute(self, query, parameters):
        """ Binds parameters to a given query and executes it non-blocking and
        asynchronously, returning an object from which the response can be
        received.

        :param query: the query to execute
        :param parameters: the parameters to bind to the query before execution
        :return: object that can receive the response
        """
        raise NotImplementedError

    def handle_respons(self, response_object):
        """ Handles the response that object will eventually get, ideally
        non-blocking.

        :param response_object: the object that will receive the response
        :return:
        """
        raise NotImplementedError