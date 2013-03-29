import re
import itertools
import pprint

from __abstractaggregator import RawAbstractAggregator

def dec_maker(param):
    if param == 0:
        def one_point_decorator(func):
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper
    if param == 1:
        def one_point_decorator(func):
            def wrapper(*args, **kwargs):
                ret = [_res for _res in func(*args, **kwargs)]
                l = (i.values()[0] for i in ret)
                t = (i.keys()[0] for i in ret)
                count = len(ret)
                print count
                if count > 0:
                    res = [ _res/count for _res in reduce(lambda x,y: map(lambda X,Y: X+Y, x,y), l)]
                    ave_time = reduce(lambda x,y: x+y, t)/count
                    print "RESULT:", ave_time, res
                    yield { ave_time : res }
                else:
                    yield None
            return wrapper

    return one_point_decorator

class AverageAggregator(RawAbstractAggregator):

    def __init__(self, **config):
        super(AverageAggregator, self).__init__()
        self.query = config['query']
        self.name = config['name']
        self._is_rps = config.has_key("rps")
        self.aggregate_group = dec_maker(1)(self.aggregate_group)
        print self.query

    def aggregate(self, timeperiod):
        normalize = (timeperiod[1] - timeperiod[0]) if self._is_rps else 1
        def format_me(i):
            try:
                ret = i[0][0]/normalize
            except Exception:
                print "Wrong type for normalization"
                pass
            else:
                return ret
        db = self.dg
        self.query = self.table_regex.sub(db.tablename, self.query)
        if self.time_regex.search(self.query):
            queries = ((self.time_regex.sub(str(time), self.query), time) for time in xrange(*timeperiod))
        else:
            queries = [(self.query, timeperiod[1])]
        l = [(format_me(db.perfomCustomQuery(query)), _time) for query, _time in queries]
        print l
        return self.name,  self._pack(l)

    def _pack(self, data):
        res = [{'time': time, 'res' : res} for res, time in data if res is not None]
        return res

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
   
    def aggregate_group(self, data):
        for sec in self._unpack(data):
            time = sec[0]
            per_subgroup_count = list()
            for subgroup in sec[1]:
                per_subgroup_count.append((sum(subgroup)))
            group_summ = sum(per_subgroup_count)
            per_subgroup_count.append(group_summ)
            print per_subgroup_count
            yield { time : per_subgroup_count }


PLUGIN_CLASS = AverageAggregator
