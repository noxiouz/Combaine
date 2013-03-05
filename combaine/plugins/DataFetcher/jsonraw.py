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
            self.port = config.get('port', 8111)
            self.url = config.get('url', "/stats")
        except Exception, err:
            self.log.error("Error in init JsonRaw getter: %s" % str(err))
            raise Exception

    def getData(self, host_name, timeperiod):
        try:
            data = urllib.urlopen("http://%(host)s:%(port)i%(url)s" % {  'host'  : host_name,
                                                                        'url'   : self.url,
                                                                        'port'  : self.port })
            _data = data.read()
            jsondata = json.loads(_data)
            if jsondata.get('Time') is None:
                jsondata['Time'] = int(0.5*(timeperiod[0]+timeperiod[1]))
            data.close()
            return (str(jsondata),)
        except Exception as err:
            self.log.error('Error while getting data with request: %s' % err)
            print str(err)
            return None

PLUGIN_CLASS = JsonRaw


