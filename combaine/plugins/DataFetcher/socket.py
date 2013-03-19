from __abstractfetcher import AbstractFetcher


import logging
import time
import socket

class _Socket(AbstractFetcher):

    def __init__(self, **config):
        self.log = logging.getLogger("combaine")
        try:
            self.port = config.get('port', 89)
        except Exception, err:
            self.log.error("Error in init Socket getter: %s" % str(err))
            raise Exception


    def getData(self, host_name, timeperiod):
        """Ignore timeperiod"""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1.0)
            s.connect((host_name, self.port))
            res = ""
            while True:
                data = s.recv(1024)
                if data == "":
                    break
                res = res + data
            s.close()
            if res == "":
                return None
            return [res,]
        except Exception as err:
            self.log.error('Error while getting data with request: %s' % err)
            return None



PLUGIN_CLASS = _Socket
