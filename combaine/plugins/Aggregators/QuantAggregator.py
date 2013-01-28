import re
import itertools
import collections
import pprint
import logging

logger = logging.getLogger("combaine")

from __abstractaggregator import AbstractAggregator

"""
    DESCRIBE THIS!!!!
"""

def coroutine(func):
    def wrapper(*args):
        f = func(*args)
        f.next()
        return f
    return wrapper

def dec_maker(param):

    if param == 1:
        def one_point(func):
            def wrapper(*args, **kwargs):
                res = [_res for _res in func(*args, **kwargs)]
                l = (i.values()[0] for i in res)
                t = (i.keys()[0] for i in res)
                count = len(res)
                if count != 0:
                    print res
                    logger.info(str(res))
                    Y = ( _res for _res in reduce(lambda x,y: map(lambda X,Y: map(lambda g,j: g+j, X,Y), x,y), l))
                    ave_time = reduce(lambda x,y: x+y, t)/count
                    ret =  [[j/count for j in k] for k in Y]
                    yield { ave_time : ret }
                else:
                    yield None
            return wrapper
        return one_point
    if param == 0:
        def one_point(func):
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper
        return one_point


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

class QuantilAggregator(AbstractAggregator):
    
    def __init__(self, **config):
        super(QuantilAggregator, self).__init__()
        self.query = config['host']
        self.name = config['name']
        print config
        self.quants = config['values']
        self.aggregate_group = dec_maker(1)(self.aggregate_group)

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
            qpack = collections.defaultdict(int)
            count = 0
            for item in iterator:
                qpack[int(item)]+=1
                count +=1
            return {"data" : sorted(qpack.iteritems()),"count" : count}

        res =  [{'time': time, 'res' :  quantile_packer(itertools.chain(*res))} for res, time in data if res is not None]
        #===================
        #with open('s.txt','a') as f:
        #    map(f.write, (str(i)+"\n" for i in res))
        return res

    def _unpack(self, data):
        """
            HEAL!!!
        """
        subgroups_count = len(data)
        data_dict = dict()
        count_dict = dict()
        for group_num, group in enumerate(data): #iter over subgroups
            # turple of first src in every host 
            for kk in (i for i in itertools.izip_longest(*group, fillvalue=None)):
                t = (j for j in kk if j is not None)
                for item in t:
                    count_dict.setdefault(item['time'], [0]*subgroups_count)[group_num] += item['res']['count']
                    for k in item['res']['data']:
                        if data_dict.get(item['time']) is None:
                            data_dict[item['time']] = list()
                            [data_dict[item['time']].append(list()) for i in xrange(0,subgroups_count)]
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
