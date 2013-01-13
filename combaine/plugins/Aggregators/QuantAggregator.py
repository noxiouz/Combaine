import re
import itertools

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
        res = []
        for group in data:
            ret = []
            for i in group:
                l =  sorted(itertools.chain(*(y["res"]["data"] for y in i)))
                count = sum(y["res"]["count"] for y in i)
                ret.append((l,count, i[0]["time"]))
            res.append(ret)
        return res

    def aggregate_group(self, data):
        ret = self._unpack(data)
#================================
        print "=================="
#  === if len() == len()
        Z = itertools.izip(*ret)
        for sec in Z:
            print sec
            count = 0
            q2 = []
            q1 = []
            Meta = QuantCalc(True)
            for agg in sec:
                M = QuantCalc()
                print agg[0]
                f = (res for res in M.quant([q*agg[1]/100 for q in self.quants], agg[0]))
                q2.append(f)
                q1.append(M)
                count += agg[1]
            t2 = list(z for z in itertools.chain(*(itertools.izip_longest(*q2))) if z is not None)
            f2 = Meta.quant([q*count/100 for q in self.quants], sorted(t2))
            print [x for x in f2]
            print [x.res for x in q1]
            print Meta.res
        return "OK"#M.res, M1.res, M2.res

PLUGIN_CLASS = QuantilAggregator
