from __abstractfetcher import AbstractFetcher


import logging
import time
import socket

class Tailer(AbstractFetcher):

    def __init__(self, **config):
        self.log = logging.getLogger("combaine")
        try:
            self.port = config['port'] if config.has_key('port') else 89
        except Exception, err:
            self.log.error("Error in init Tailer getter: %s" % str(err))
            raise Exception


    def getData(self, host_name, timeperiod):
        """Ignore timeperiod"""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host_name, self.port))
            res=""
            while True:
                data = s.recv(512)
                if data == "":
                    break
                res = res + data
            s.close()
            if res == "":
                return None
            d = {}
            for i in res.splitlines():
                try:
                    key, value = i.split('=')
                except:
                    pass
                else:
                    d[key] = value if value !="" else 0
            return (str(d),)
        except Exception as err:
            self.log.error('Error while getting data with request: %s' % err)
            return None


PLUGIN_CLASS = Tailer
