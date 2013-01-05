from __abstractfetcher import AbstractFetcher

import logging
import time
import json
import urllib

class JsonRaw(AbstractFetcher):
        
    def __init__(self, **config):
        self.log = logging.getLogger("combaine")
        self.filter = lambda x: True
        try:
            self.port = config['port'] if config.has_key('port') else 8111
            self.url = config['url'] if config.has_key('port') else "/stats"
        except Exception, err:
            self.log.error("Error in init JsonRaw getter: %s" % str(err))
            raise Exception

    def getData(self, host_name, timeperiod):
        try:
            data = urllib.urlopen("http://%(host)s:%(port)i%(url)" % {  'host'  : host_name,
                                                                        'url'   : self.url,
                                                                        'port'  : self.port })
            jsondata = json.load(data)
            if jsondata.get('Time') is None:
                jsondata['Time'] = int(0.5*(timeperiod[0]+timeperiod[1]))
            data.close()
            return (str(jsondata),)
        except Exception as err:
            self.log.error('Error while getting data with request: %s' % err)
            return None

PLUGIN_CLASS = JsonRaw
