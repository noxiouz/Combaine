import re
import itertools
import collections
import logging


from __abstractaggregator import RawAbstractAggregator
from combaine.common.loggers import CommonLogger

def coroutine(func):
    def wrapper(*args):
        f = func(*args)
        f.next()
        return f
    return wrapper

class QuantCalc(object):
    """
        HARD TEST!!! Repair description:
        ===============================
        Class for calculation quantile from iterator over list like this:
            [
                (value1, count1),
                (value2, count2),
                (value3, count3)
            ]
        If is_stopped == False (default),
            then stop generation at the end of iterator
        Otherwise stopped by size of result
    """

    def __init__(self, is_stopped=False):
        self.res = []
        self.is_stopped = is_stopped

    @coroutine
    def quant(self, qts, it):
        qts.sort(reverse=True)
        if len(qts) == 0:
            yield None
            return
        size = len(qts)
        lim = qts.pop()
        summ = 0
        yield
        for i in it:
            summ += i[1]
            if summ > lim:
                if len(qts) > 0:
                    lim = qts.pop()
                if len(self.res) < size:
                    self.res.append(i[0])
                    if (len(self.res) == size) and (self.is_stopped):
                        yield None
                        return
            yield i

class QuantilAggregator(RawAbstractAggregator):
    
    def __init__(self, **config):
        super(QuantilAggregator, self).__init__()
        self.logger = CommonLogger()
        self.query = config['query']
        self.name = config['name']
        self.quants = config['values']

    def aggregate(self, timeperiod):
        db = self.dg
        self.query = self.table_regex.sub(db.tablename, self.query)
        self.query = self.time_regex.sub("1=1", self.query) # DEPRECATED: Only for backward compability
        data = (db.perfomCustomQuery(self.query), timeperiod[1])
        return self.name, self._pack(data)

    def _pack(self, data):
        def quantile_packer(iterator):
            qpack = collections.defaultdict(int)
            count = 0
            for item in iterator:
                qpack[int(item)] += 1
                count += 1
            if len(qpack.keys()) == 0:
                qpack[0] = 0 #Special for vyacheslav
            return {"data" : sorted(qpack.iteritems()), "count" : count}

        res =  {'time': data[1], 'res' :  quantile_packer(itertools.chain(*data[0]))}
        return res

    def _unpack(self, data):
        subgroups_count = len(data)
        data_dict = dict()
        count_dict = dict()
        for group_num, group in enumerate(data): #iter over subgroups
            for item in (k for k in group if k is not None):
                count_dict.setdefault(item['time'], [0]*subgroups_count)[group_num] += item['res']['count']
                for k in item['res']['data']:
                    if data_dict.get(item['time']) is None:
                        data_dict[item['time']] = list()
                        [data_dict[item['time']].append(list()) for i in xrange(0, subgroups_count)]
                    data_dict[item['time']][group_num].append(k)
        data_sec = data_dict.iteritems()
        count_sec = count_dict.iteritems()
        ret = itertools.izip(data_sec, count_sec)
        return ret

    def _normalize_broken_quant(self, broken_list):
        #
        # DIRTY HACK
        if len(broken_list) == len(self.quants):
            return broken_list
        if len(broken_list) == 0:
            broken_list = [0] * len(self.quants)
        else:
            broken_list = broken_list + [broken_list[-1]] * (len(self.quants) - len(broken_list))
        return broken_list

    def aggregate_group(self, data):
        for sec in self._unpack(data):
            time = sec[0][0]
            count = 0
            quant_generators = []
            quant_objects = []
            Meta = QuantCalc()
            for num_agg, agg in enumerate(sec[0][1]):
                agg.sort()
                M = QuantCalc()
                incr_count = sec[1][1][num_agg]
                f = (res for res in M.quant([q*sec[1][1][num_agg]/100 for q in self.quants], agg) )
                quant_generators.append(f)
                quant_objects.append(M)
                count += incr_count
            t2 = (z for z in itertools.chain(*(itertools.izip_longest(*quant_generators))) if z is not None)
            f2 = Meta.quant([q*count/100 for q in self.quants], sorted(t2))
            [yy for yy in f2]
            quant_objects.append(Meta)
            yield {time : [self._normalize_broken_quant(x.res) for x in quant_objects] } # yield????

PLUGIN_CLASS = QuantilAggregator
