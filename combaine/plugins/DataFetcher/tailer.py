from __abstractfetcher import AbstractFetcher
from combaine.common.loggers import CommonLogger

import time
import socket
import json

class Tailer(AbstractFetcher):

    def __init__(self, **config):
        self.log = CommonLogger()
        self.filter = lambda x: True
        try:
            self.port = config['port'] if config.has_key('port') else 89
        except Exception, err:
            print err
            self.log.error("Error in init Tailer getter: %s" % str(err))
            raise Exception


    def getData(self, host_name, timeperiod):
        """Ignore timeperiod"""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1.0)
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
                    try:
                        value = float(value) if '.' in value else int(value)
                    except ValueError:
                        pass
                except:
                    pass
                else:
                    d[key] = value if value !="" else 0
            d['time'] = int(0.5*(timeperiod[0]+timeperiod[1]))
            return (json.dumps(d),)
        except Exception as err:
            print err
            self.log.error('Error while getting data with request: %s' % err)
            return None



PLUGIN_CLASS = Tailer
