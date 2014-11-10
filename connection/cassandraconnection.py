from datetime import datetime
from time import sleep

from cassandra.cluster import Cluster

from connectioninterface import ConnectionInterface


class CassandraConnection(ConnectionInterface):
    cluster = None
    session = None
    execute_unprepared_stmt = None
    prepare = None

    def __init__(self, **kwargs):
        ConnectionInterface.__init__(self, **kwargs)

    def connect(self, **kwargs):
        """ Create connection to cassandra cluster.

        :param kwargs: keyword arguments for cassandra.cluster.Cluster
        """
        self.cluster = Cluster(**kwargs)
        self.session = self.cluster.connect()

        # method to execute unprepared statements
        self.execute_unprepared_stmt = self.session.execute
        # method to prepare statements
        self.prepare = self.session.prepare

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

        # execute query asynchronously, returning a ResponseFuture-object
        # to which callbacks can be added
        #print 'Statement: %s Parameters: %s' % (statement, parameters)
        future = self.session.execute_async(statement, trace=True)
        #print 'resulting query: %s request_id: %s' % (future.query, future._req_id)
        # Add a callback to fn which puts data needed by the LogGenerator into
        # the queue. The errback calls the stated function with the error as
        # first positional parameter, the normal callback just calls it with
        # the given parameters. To handle both cases the wrapper function fn is
        # used, which gets 'None' as first parameter in the non-error-case.

        # start = datetime.now()
        # try:
        #     response = future.result()
        # except Exception as e:
        #     print e

        #print 'Trace: %s' % future.get_query_trace()
        #queue_out.put((response, start, datetime.now(), metadata))
        future.add_callbacks(callback=self.success, callback_args=(
                            datetime.now(), metadata, queue_out),
                            errback=self.failure, errback_args=(
                            (datetime.now(), metadata, queue_out))
                            )

    def success(self, response, start, mdata, queue_out):
        # print 'CassandraConnection success: ', None, start, datetime.now(), mdata
        queue_out.put((response, start, datetime.now(), mdata))

    def failure(self, response, start, mdata, queue_out):
        # print 'CassandraConnection failure: ', response, start, datetime.now(), mdata
        # response = 'ERROR! Message: %s Errors: %s' % (response.message, response.errors)
        queue_out.put((response, start, datetime.now(), mdata))

    def fn(self, err, start, mdata, queue):
        print 'CassandraConnection: ', err, start, datetime.now(), mdata
        queue.put((err, start, datetime.now(), mdata))