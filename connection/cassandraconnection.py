from time import time
from cassandra.cluster import Cluster
from interface import Interface

class CassandraConnection(Interface):

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
        """ Execute prepared statement with parameters.
         The query is executed non-blocking and asynchronously,
         automatically logging the result.

        :param cassandra.query.Statement statement: the prepared statement
        :param parameters: parameters for the prepared statement
        """

        future = self.session.execute_async(statement,parameters)
        future.add_callbacks(
            callback=log_results, callback_kwargs={'start_time':time()},
            errback=log_err, errback_kwargs={'statement':statement})