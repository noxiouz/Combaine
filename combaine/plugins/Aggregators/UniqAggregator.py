from __abstractaggregator import ProcessedAbstractAggregator

from pprint import pprint
import collections
import itertools

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
        res = res.difference(set(self.skipvalues))
        return self.name, self._pack(res, timeperiod[0])

    def aggregate_group(self, data):
        for sec in self._unpack(data):
            time = sec[0]
            per_sbgp = list()
            for sbgp in sec[1]:
                per_sbgp.append(set(itertools.chain(*sbgp)))
            meta_group_set = set()
            [meta_group_set.update(x) for x in per_sbgp]
            per_sbgp.append(meta_group_set)
            yield { time : [len(x) for x in per_sbgp]}

    def _pack(self, data, time):
        return [{'time': time, 'res': list(data)},]


    def _unpack(self, data):
        subgroups_count = len(data)
        data_dict = dict()
        for group_num, group in enumerate(data): #iter over subgroups
            for k in (i for i in itertools.izip_longest(*group, fillvalue=None)):
                t = (j for j in k if j is not None)
                for item in t:
                    if data_dict.get(item['time']) is None:
                        data_dict[item['time']] = list()
                        [data_dict[item['time']].append(list()) for i in xrange(0,subgroups_count)]
                    data_dict[item['time']][group_num].append(item['res'])
        data_sec = data_dict.iteritems()
        return data_sec

PLUGIN_CLASS = Uniq
