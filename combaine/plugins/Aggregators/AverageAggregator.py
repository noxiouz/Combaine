import re
import itertools

from __abstractaggregator import RawAbstractAggregator
from combaine.common.loggers import CommonLogger

class AverageAggregator(RawAbstractAggregator):

    def __init__(self, **config):
        self.logger = CommonLogger()
        super(AverageAggregator, self).__init__()
        self.query = config['query']
        self.name = config['name']
        self._is_rps = config.get("rps", "YES")

    def aggregate(self, timeperiod):
        normalize = (timeperiod[1] - timeperiod[0]) if self._is_rps == "YES" else 1
        def format_me(i):
            try:
                ret = i[0][0]/normalize
            except Exception:
                #self.logger.exception("Wrong type for normalization")
                # May be invalid format - so drop it
                pass
            else:
                return ret
        db = self.dg
        self.query = self.table_regex.sub(db.tablename, self.query)
        self.query = self.time_regex.sub("1=1", self.query) # Only for backward compability
        l = (format_me(db.perfomCustomQuery(self.query)), timeperiod[1])
        self.logger.debug("Result of %s aggreagtion: %s" % (self.name, l))
        return self.name,  self._pack(l)

    def _pack(self, data):
        res = {'time': data[1], 'res' : data[0]}
        return res

    def _unpack(self, data):
        subgroups_count = len(data)
        data_dict = dict()
        for group_num, group in enumerate(data): #iter over subgroups
            for item in group:
                if data_dict.get(item['time']) is None:
                    data_dict[item['time']] = list()
                    [data_dict[item['time']].append(list()) for i in xrange(0,subgroups_count)]
                data_dict[item['time']][group_num].append(item['res'])
        data_sec = data_dict.popitem()
        return data_sec
   
    def aggregate_group(self, data):
        sec, value = self._unpack(data)
        per_subgroup_count = list()
        for subgroup in value:
            per_subgroup_count.append((sum(subgroup)))
        group_summ = sum(per_subgroup_count)
        per_subgroup_count.append(group_summ)
        self.logger.debug("%s: %s" % (self.name, per_subgroup_count))
        yield { sec : per_subgroup_count }


PLUGIN_CLASS = AverageAggregator
