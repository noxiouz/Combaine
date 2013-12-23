import re
import itertools

from __abstractaggregator import RawAbstractAggregator
from combaine.common.loggers import CommonLogger
from collections import defaultdict

import sys
sys.path.append('/usr/lib')

from jclient import config
from jclient import jobs

class ErrCountAggregator(RawAbstractAggregator):

    def __init__(self, **config):
        self.logger = CommonLogger()
        super(ErrCountAggregator, self).__init__()
        self.name = config['name']
        self.q_head = "SELECT http_host, SUBSTRING_INDEX(geturl, '?', 1), COUNT(*) from %TABLENAME%"
        self.q_tail = "GROUP BY http_host, SUBSTRING_INDEX(geturl, '?', 1)"
        self.q_blacklist = " and ".join(["http_host != '%(i)s' and SUBSTRING_INDEX(geturl, '?', 1) != '%(i)s'" % {'i': i} 
                                         for i in config["blacklist"]])
        self.check_name = config["monitoring"]["name"] # name of juggler event
        self.check_code = tuple(config["monitoring"]["code"]) # response code >= low and < high
        self._limits = tuple(config["limits"].pop("default"))
        self.limits = dict([ (key, tuple(val)) for key, val in config["limits"].items() ])

    def _query(self, code=None):
        q = [self.q_head]
        if code or self.q_blacklist:
            q.append("WHERE")
            if code: q.append("http_status >= %s and http_status < %s" % code)
            if code and self.q_blacklist: q.append("and")
            if self.q_blacklist: q.append(self.q_blacklist)

        q.append(self.q_tail)
        q = " ".join(q)
        return self.table_regex.sub(self.dg.tablename, q)

    def aggregate(self, host_name, group_name, timeperiod):
        config.loadConfigs()
        def send_juggler(msg):
            print host_name, msg
            return
            status = {0 : "OK", 1 : "WARN", 2 : "CRIT"}
            st, dsc = msg
            st = status[st]
            self.logger.debug("%s %s %s %s" % (host_name, self.check_name, st, dsc))
            if not jobs.addJobs(host_name, self.check_name, st, dsc):
                self.logger.error("Can't send data to juggler")
            
        db = self.dg
        rekey = lambda val: ((val[0],val[1]), val[2]) 
        reqs_all = dict(map(rekey, db.perfomCustomQuery(self._query())))
        reqs_err = dict(map(rekey, db.perfomCustomQuery(self._query(self.check_code))))
        err_prct = ( (handler,
                      total,
                      reqs_err.get(handler, 0), 
                      round(
                             float(reqs_err.get(handler, 0)) / total * 100,
                             1)
                     ) for handler, total in reqs_all.items() )

        self.logger.debug("%s %s%%" % (host_name, 
                                     float(sum(reqs_err.values())) / sum(reqs_all.values()) * 100))

        juggler_msg = (0, "Ok")
        for handler, requests, errors, percents in err_prct:
            vhost = handler[0]
            handler = ''.join(handler)
            max_errs, min_reqs = self.limits.get(vhost, self._limits)
            #print handler, percents, max_errs, requests, min_reqs
            if percents >= max_errs and requests <= min_reqs:
                #warning
                msg = "%s - %s%% (%s/%s)" % (handler, percents, errors, requests)
                juggler_msg = (1, msg) if juggler_msg[0] < 2 else juggler_msg
            elif percents >= max_errs and requests > min_reqs:
                #critical
                msg = "%s - %s%% (%s/%s)" % (handler, percents, errors, requests)
                juggler_msg = (2, msg)

            if errors:
                #send to dashboard
                pass

        send_juggler(juggler_msg)

    def _pack(self, data):
        pass

    def _unpack(self, data):
        pass
   
    def aggregate_group(self, data):
        raise StopIteration

PLUGIN_CLASS = ErrCountAggregator
