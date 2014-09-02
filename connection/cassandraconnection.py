from datetime import datetime
from cassandra.cluster import Cluster
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

    def execute(self, statement, parameters, workload_id, wl_query_id):
        """ Execute prepared statement with parameters.
         The query is executed non-blocking and asynchronously,
         automatically logging the result.

        :param cassandra.query.Statement statement: the prepared statement
        :param parameters: parameters for the prepared statement
        :param workload_id: workload identifier for logging
        :param wl_query_id: workload query identifier for logging
        """

        # execute query asynchronously, returning a ResponseFuture-object
        # to which callbacks can be added
        # TODO: set trace to True?
        future = self.session.execute_async(statement, parameters)
        future.add_callbacks(
            callback=logger.log_results, callback_kwargs={
                                                    'time_start': datetime.now(),
                                                    'workload_id': workload_id,
                                                    'wl_query_id': wl_query_id},
            errback=logger.log_err, errback_kwargs={
                                                'statement': statement,
                                                'parameters': parameters,
                                                'workload_id': workload_id,
                                                'wl_query_id': wl_query_id}
        )
