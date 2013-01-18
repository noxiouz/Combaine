import re
import itertools
import collections

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
        print "QUANTS:", qts
        if len(qts) == 0:
            yield None
            return
        size = len(qts)
        lim = qts.pop()
        summ = 0
        yield
        for i in it:
            summ += i[1]
            print "SUMM:", summ, i[1], i[0] 
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

    def aggregate(self, db, timeperiod):
        self.query = self.table_regex.sub(db.tablename, self.query)
        if self.time_regex.search(self.query):
            queries = ((self.time_regex.sub(str(time), self.query), time) for time in xrange(*timeperiod))
        else:
            queries = (self.query, timeperiod[1])
        data = ((db.perfomCustomQuery(query), time) for query, time in queries)
        print "AAAAA"
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
            for k in (i for i in itertools.izip_longest(*group, fillvalue=None)):
                t = (j for j in k if j is not None)
                for item in t:
                    #z = itertools.chain(item['res']['data'])
                    count_dict.setdefault(item['time'], [0]*subgroups_count)[group_num]+= item['res']['count']
                    [data_dict.setdefault(item['time'], [[]]*subgroups_count)[group_num].append(k) for k in item['res']['data']]
        data_sec = data_dict.iteritems()
        count_sec = count_dict.iteritems()
        ret = itertools.izip(data_sec, count_sec)
        return ret

    def aggregate_group(self, data):
        for sec in self._unpack(data):
            time = sec[0][0]
            print "time: ",time
            count = 0
            quant_generators = []
            quant_objects = []
            Meta = QuantCalc(True)
            for num_agg, agg in enumerate(sec[0][1]):
                agg.sort()
                M = QuantCalc()
                incr_count =  sec[1][1][num_agg]
                print "incr:",incr_count
                f = (res for res in M.quant([q*sec[1][1][num_agg]/100 for q in self.quants], agg))
                quant_generators.append(f)
                quant_objects.append(M)
                count += incr_count
                print agg
                print "count: ",count
            t2 = (z for z in itertools.chain(*(itertools.izip_longest(*quant_generators))) if z is not None)
            f2 = Meta.quant([q*count/100 for q in self.quants], sorted(t2))
            [x for x in f2]
            Meta.res
            yield {time : ([x.res for x in quant_objects], Meta.res) } # yield????

PLUGIN_CLASS = QuantilAggregator
