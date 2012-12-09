
import logging
import time

class AbstractGetter(object):
    
    def __init__(self):
        raise Exception

    def getData(self):
        """ Must return a generator object """
        raise Exception

    filter = None

import httplib

class Timetail(AbstractGetter):

    def __init__(self, **config):
        self.log = logging.getLogger('combaine')
        try:
            url = config['timetail_url']
            self.port = config['timetail_port'] if config.has_key('timetail_port') else 3132
            log_name = config['logname']
            self.http_get_url = "%(url)s%(log)s&time=" % { 'url' : url, 'log' : log_name }
        except Exception, err:
            self.log.error("Error in init Timetail getter: %s" % str(err))
            print "Error in init Timetail getter: %s" % str(err)
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
                log.warn('HTTP responce code is not 200')
                return None
        except Exception, err:
            self.log.error('Error while getting data with request: %s' % err)
            print err
            return None

def GetterFactory(**config):
    types = { 'timetail' : Timetail,
              'raw'      : None
    }
    return  types[ config["type"]](**config)
