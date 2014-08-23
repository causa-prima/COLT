from collections import defaultdict
from datetime import datetime

class Logger(object):
    """
    :param optional float bucket_base: base of bucket size. each bucket is a power of base. default=1.2
    :param optional int time_div: time is measured in microseconds div time_div. default=1000
    """

    def __init__(self, bucket_base=1.2, time_div=1000):

        # A dict for every workload containing the logged values.
        # Using defaultdict for easy of inserting new items as one does not
        # need to check if a key is already in the dict.
        # To add a value to a certain bucket: self.stats[workload_id][query_id][bucket] += value
        # Arbitrary info can also be stored on any level, e.g.
        #               self.stats[text] = 'foo'
        # or            self.stats[workload_id][info] = 'foo'
        self.stats = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        self.bucket_base = bucket_base
        self.time_div = time_div


    def log_result(self, time_start, workload_id, query_id):
        """ Log the time needed for a given query.

        :param datetime.datetime time_start: time of query start
        :param workload_id: id of the workload the query belongs to
        :param query_id: id of the query
        """
        dur = datetime.now() - time_start
        dur = (dur.seconds * 1000000 + dur.microseconds)/self.time_div
        # compute the bucket the duration falls into
        bucket = (int(pow(self.bucket_base, int(log(dur, self.bucket_base)))))
        self.stats[workload_id][query_id][bucket] += 1

    def log_err(self):
        pass