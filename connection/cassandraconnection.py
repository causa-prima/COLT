from datetime import datetime

from cassandra.cluster import Cluster

from connectioninterface import ConnectionInterface


class CassandraConnection(ConnectionInterface):
    cluster = None
    session = None
    execute_unprepared_stmt = None

    def connect(self, **kwargs):
        """ Create connection to cassandra cluster.

        :param kwargs: keyword arguments for cassandra.cluster.Cluster
        """
        self.cluster = Cluster(**kwargs)
        self.session = self.cluster.connect()

        # method to execute unprepared statements
        self.execute_unprepared_stmt = self.session.execute_async

    def shutdown(self):
        """ Terminate connection to cassandra cluster.
        """
        self.cluster.shutdown()

    def execute(self, statement, parameters, queue_out, metadata=None):
        """ Executes a prepared statement after binding given parameters.
         The query is executed non-blocking and asynchronously.

        :param cassandra.query.PreparedStatement statement: the prepared statement
        :param list parameters: parameters to bind to the prepared statement
        :param multiprocessing.Queue queue_out: the queue in which to put the results
        :param tuple metadata: metadata needed for logging. default = None
        """

        # we need to wrap the queue.put() function to handle both callbacks
        # (see there for further explanation)
        fn = lambda err, start, mdata: queue_out.put((err, start, datetime.now(), mdata))
        # execute query asynchronously, returning a ResponseFuture-object
        # to which callbacks can be added
        future = self.session.execute_async(statement, parameters)
        # Add a callback to fn which puts data needed by the LogGenerator into
        # the queue. The errback calls the stated function with the error as
        # first positional parameter, the normal callback just calls it with
        # the given parameters. To handle both cases the wrapper function fn is
        # used, which gets 'None' as first parameter in the non-error-case.
        future.add_callbacks(callback=fn, callback_args=(
                            None, datetime.now(), metadata),
                            errback=fn, errback_args=(
                            (datetime.now(), metadata))
                            )