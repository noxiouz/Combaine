import re
import itertools

from __abstractaggregator import AbstractAggregator


class QuantilAggregator(AbstractAggregator):
    
    def __init__(self, **config):
        super(QuantilAggregator, self).__init__()
        self.query = config['host']
        self.name = config['name']
        print self.query

    def aggregate(self, db, timeperiod):
        self.query = self.table_regex.sub(db.tablename, self.query)
        if self.time_regex.search(self.query):
            queries = ((self.time_regex.sub(str(time), self.query), time) for time in xrange(*timeperiod))
        else:
            queries = (self.query, timeperiod[1])
#        data = ([int(l) for l in itertools.chain(*db.perfomCustomQuery(query))] for query, time in queries)
        data = ((db.perfomCustomQuery(query), time) for query, time in queries)
        return self.name, self._pack(data)

    def _pack(self, data):

        def quantile_packer(iterator):
            import collections
            qpack = collections.defaultdict(int)
            count = 0
            for item in iterator:
                qpack[int(item)]+=1
                count +=1
            return sorted(qpack.iteritems()), count

        res = [{'time': time, 'res' :  quantile_packer(itertools.chain(*res))} for res, time in data if res is not None]
        print res
        return res

    def _unpack(self, data):
        pass

PLUGIN_CLASS = QuantilAggregator

