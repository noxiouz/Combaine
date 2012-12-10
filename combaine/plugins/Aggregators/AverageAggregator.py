import re

from __abstractaggregator import AbstractAggregator


class AverageAggregator(AbstractAggregator):

    def __init__(self, **config):
        super(AverageAggregator, self).__init__()
        self.query = config['host']
        self.name = config['name']
        print self.query

    def aggregate(self, db, timeperiod):
        self.query = self.table_regex.sub(db.tablename, self.query)
        if self.time_regex.search(self.query):
            queries = ((self.time_regex.sub(str(time), self.query), time) for time in xrange(*timeperiod))
        else:
            queries = (self.query, timeperiod[1])
        return ({ 'name' : self.name, 'time': time, 'data' : db.perfomCustomQuery(query)[0][0]} for query, time in queries)


PLUGIN_CLASS = AverageAggregator


