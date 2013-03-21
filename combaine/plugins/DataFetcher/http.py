from __abstractfetcher import AbstractFetcher

import logging
import time
import httplib


class _HTTP(AbstractFetcher):
    
    def __init__(self, **config):
        self.log = logging.getLogger("combaine")
        try:
            url = config.get('url','')
            self.port = config.get('port', 3132)
            self.http_get_url = "%(url)s" % { 'url' : url}
        except Exception, err:
            self.log.error("Error in init HTTP Fetcher: %s" % str(err))
            raise Exception

    def getData(self, host_name, timeperiod):
        try:
            req = "%s" % self.http_get_url
            self.log.info('Get data by request: %s' % req)
            conn = httplib.HTTPConnection(host_name, self.port, timeout=2)
            conn.request("GET", req, None)
            resp = conn.getresponse()
            if resp.status == 200:
                self.log.info("Receive %s bytes" % resp.getheader("Content-Length"))
                _ret = (line for line in resp.read().splitlines())
                conn.close()
                return _ret
            else:
                self.log.warning('HTTP responce code for %s is not 200 %i' % (host_name, resp.status))
                return None
        except Exception, err:
            self.log.error('Error while getting data with request: %s' % err)
            return None

PLUGIN_CLASS = _HTTP
