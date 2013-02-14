from __abstractaggregator import ProcessedAbstractAggregator

from pprint import pprint
import collections

class Uniq(ProcessedAbstractAggregator):

    def __init__(self, **config):
        super(Uniq, self).__init__()
        self.query = config["query"]
        self.name = config["name"]
        self.terminator = config["terminator"]
        self.skipvalues = config["skipvalues"]

    def aggregate(self, timeperiod):
        res = set()
        for line in self.data:
            [res.add(i) for i in str(line.get(self.query,"")).split(self.terminator)]
        res.difference(set(self.skipvalues))
        return self.name, self._pack(res, timeperiod[0])

    def aggregate_group(self, data):
        data = self._unpack(data)
        for sec, value in data.iteritems():
            yield { sec : len(set(value))}

    def _pack(self, data, time):
        return [{'time': time, 'res': list(data)}, ]


    def _unpack(self, data):
        ret = collections.defaultdict(list)
        for l in data:
            for j in l:
                for x in j:
                    ret[x['time']] += x['res']
        return ret



PLUGIN_CLASS = Uniq
