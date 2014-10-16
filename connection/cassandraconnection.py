from datetime import datetime

from cassandra.cluster import Cluster
from cassandra import OperationTimedOut

from connectioninterface import ConnectionInterface
from logger import Logger

logger = Logger()

class CassandraConnection(ConnectionInterface):

    def connect(self, **kwargs):
        """ Create connection to cassandra cluster.

        :param kwargs: keyword arguments for cassandra.cluster.Cluster
        """
        self.cluster = Cluster(**kwargs)
        self.session = self.cluster.connect()

    def shutdown(self):
        """ Terminate connection to cassandra cluster.
        """
        self.cluster.shutdown()

    def execute(self, statement, parameters):
        """ Executes a prepared statement after binding given parameters.
         The query is executed non-blocking and asynchronously.

        :param cassandra.query.PreparedStatement statement: the prepared statement
        :param list parameters: parameters to bind to the prepared statement
        :return: ResponseFuture object for handling the result, entangled with the timestamp of the query execution
        :rtype: Tuple(ResponseFuture, datetime.datetime)
        """

        # execute query asynchronously, returning a ResponseFuture-object
        # to which callbacks can be added
        return self.session.execute_async(statement, parameters), datetime.now()

    def handle_response(self, response_object):
        """ Waits for a response and logs the time of its arrival.

        :param Tuple response_object: ResponseFuture object and the time of the query execution
        """
        future, start = response_object
        try:
            # wait until the response arrived or the default timeout strikes
            future.result()
            return start - datetime.now()
        except OperationTimedOut as OTO:
            msg = "Waiting for the response of query '%s' timed out.\n" % future.query
            raise Warning(msg, OTO)
            return None
