from __abstractfetcher import AbstractFetcher

import logging
import time
import httplib

class Timetail(AbstractFetcher):

    def __init__(self, **config):
        self.log = logging.getLogger('combaine')
        try:
            url = config['timetail_url']
            self.port = config['timetail_port'] if config.has_key('timetail_port') else 3132
            log_name = config['logname']
            self.http_get_url = "%(url)s%(log)s&time=" % { 'url' : url, 'log' : log_name }
        except Exception, err:
            self.log.error("Error in init Timetail getter: %s" % str(err))
            raise Exception

    def getData(self, host_name, timeperiod):
        try:
            self.filter = lambda item: item['Time'] < timeperiod[1]
            req = "%s%i" % (self.http_get_url, int(time.time()) - timeperiod[0])
            self.log.info('Get data by request: %s' % req)
            conn = httplib.HTTPConnection(host_name, self.port, timeout=3)
            conn.request("GET", req, None)
            resp = conn.getresponse()
            if resp.status == 200:
                #_ret = resp.read()
                _ret = (line for line in resp.read().splitlines())
                conn.close()
                return _ret
            else:
                self.log.warning('HTTP responce code is not 200 %i' % resp.status)
                return None
        except Exception, err:
            self.log.error('Error while getting data with request: %s' % err)
            return None

PLUGIN_CLASS = Timetail
