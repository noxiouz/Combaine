import hashlib
import string
import random
from cPickle import dumps as PACK
from cPickle import loads as UNPACK

import requests

from __AbstractStorage import AbstractDistributedStorage
from combaine.common.loggers import CommonLogger


class Elliptics(AbstractDistributedStorage):

    def __init__(self, **config):
        self.logger = CommonLogger()
        
        cfg = [tuple(_i.split(":")) for _i in config["proxy_hosts"]]
        random.shuffle(cfg)
        self.hostsinfo = cfg

        self.read_timeout = config.get("read_timeout", 0.5)
        self.write_timeout = config.get("write_timeout", 0.5)

        self.read_url = string.Template("http://${HOST}:${R_PORT}/get/${KEY}?ioflags=3072")
        self.write_url = string.Template("http://${HOST}:${W_PORT}/upload/${KEY}?ioflags=3072")

    def connect(self, namespace):
        return True

    def insert(self, raw_key, data):
        key = hashlib.md5(raw_key).hexdigest()
        for host, r_port, w_port in self.hostsinfo[:]:
            try:
                r = requests.post(self.write_url.substitute(KEY=key, HOST=host, W_PORT=w_port), data=PACK(data), timeout=self.write_timeout)
                if r.status_code == 200: #because elliptics write cache bug
                    self.logger.debug("Elliptics: insert key %s (%s) succesfully" % (key, raw_key))
                    return True
            except requests.exceptions.Timeout as err:
                self.hostsinfo.remove((host, r_port, w_port))
            except requests.exceptions.ConnectionError as err:
                self.logger.debug("Elliptics hosts: %s" % self.hostsinfo)
                self.hostsinfo.remove((host, r_port, w_port))
        self.logger.error("Elliptics: failed to insert key %s (%s)" % (key, raw_key))
        return False

    def read(self, raw_key, cache=False):
        key =  hashlib.md5(raw_key).hexdigest()
        for host, r_port, w_port in self.hostsinfo[:]:
            try:
                r = requests.get(self.read_url.substitute(KEY=key, HOST=host, R_PORT=r_port), timeout=self.read_timeout)
                if r.ok:
                    self.logger.debug("Elliptics: read key %s (%s) succesfully" % (key, raw_key))
                    ret = UNPACK(r.content)
                    r.close()
                    return ret
                elif r.status_code == 404:
                    self.logger.debug("Elliptics: Key %s (%s) is missing" % (key, raw_key))
                    return None
            except requests.exceptions.Timeout as err:
                self.hostsinfo.remove((host, r_port, w_port))
            except requests.exceptions.ConnectionError as err:
                self.hostsinfo.remove((host, r_port, w_port))
            except Exception as err:
                self.logger.error("Read error in elliptics proxy %s" % err)
        self.logger.error("Elliptics: failed to read key %s (%s)" % (key, raw_key))
        return None

    def remove(self, key):
        return "OK"

    def close(self):
        return True

PLUGIN_CLASS = Elliptics
