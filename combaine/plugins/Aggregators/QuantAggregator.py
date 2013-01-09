import re
import itertools

from __abstractaggregator import AbstractAggregator


class QuantilAggregator(AbstractAggregator):
    
    def __init__(self, **config):
        super(QuantilAggregator, self).__init__()
        self.query = config['host']
        self.name = config['name']
        self.quants = config.get('values')

    def aggregate(self, db, timeperiod):
        self.query = self.table_regex.sub(db.tablename, self.query)
        if self.time_regex.search(self.query):
            queries = ((self.time_regex.sub(str(time), self.query), time) for time in xrange(*timeperiod))
        else:
            queries = (self.query, timeperiod[1])
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
        return [{'time': time, 'res' :  quantile_packer(itertools.chain(*res))} for res, time in data if res is not None]
        #=== FOR TEST HOOK
        #with open('for_test.txt','a') as f:
        #    map(f.write,(str(i)+"\n" for i in res))

    def _unpack(self, data):
        return data

    def aggregate_group(self, data):
        ud = self._unpack(data)

PLUGIN_CLASS = QuantilAggregator
