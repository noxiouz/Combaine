import re

from __abstractaggregator import AbstractAggregator


class AverageAggregator(AbstractAggregator):

    def __init__(self, **config):
        super(AverageAggregator, self).__init__()
        self.query = config['host']
        self.name = config['name']
        print self.query

    def aggregate(self, db, timeperiod):
        def format_me(i):
            try:
                ret = i[0][0]
            except Exception:
                pass
            else:
                return ret

        self.query = self.table_regex.sub(db.tablename, self.query)
        if self.time_regex.search(self.query):
            queries = ((self.time_regex.sub(str(time), self.query), time) for time in xrange(*timeperiod))
        else:
            queries = (self.query, timeperiod[1])
        l = ((format_me(db.perfomCustomQuery(query)), _time) for query, _time in queries)
        return ({ 'name' : self.name, 'time': time, 'data' : res} for res, time in l if res is not None)




PLUGIN_CLASS = AverageAggregator


