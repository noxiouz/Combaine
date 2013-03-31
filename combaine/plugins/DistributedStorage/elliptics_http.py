from __AbstractStorage import AbstractDistributedStorage

import requests
import hashlib
#import msgpack
from cPickle import dumps as PACK
from cPickle import loads as UNPACK

import string
import random

from time import sleep

class Elliptics(AbstractDistributedStorage):

    def __init__(self, **config):
        cfg = [tuple(_i.split(":")) for _i in config["proxy_hosts"]]
        random.shuffle(cfg)
        self.hostsinfo = cfg
        self.read_url = string.Template("http://${HOST}:${R_PORT}/get/${KEY}?ioflags=3072")
        self.write_url = string.Template("http://${HOST}:${W_PORT}/upload/${KEY}?ioflags=3072")

    def connect(self, namespace):
        return True

    def insert(self, key, data):
        key =  hashlib.md5(key).hexdigest()
        for host, r_port, w_port in self.hostsinfo[:]:
            try:
                r = requests.post(self.write_url.substitute(KEY=key, HOST=host, W_PORT=w_port), data=PACK(data), timeout=1)
                if r.status_code == 503: #because elliptics write cache bug
                    return True
            except requests.exceptions.Timeout as err:
                self.hostsinfo.remove((host, r_port, w_port))
            except requests.exceptions.ConnectionError as err:
                print self.hostsinfo
                self.hostsinfo.remove((host, r_port, w_port))
        return False

    def read(self, key, cache=False):
        key =  hashlib.md5(key).hexdigest()
        for host, r_port, w_port in self.hostsinfo[:]:
            try:
                r = requests.post(self.read_url.substitute(KEY=key, HOST=host, R_PORT=r_port), timeout=1)
                if r.ok:
                    ret = UNPACK(r.content)
                    r.close()
                    return ret
            except requests.exceptions.Timeout as err:
                self.hostsinfo.remove((host, r_port, w_port))
            except requests.exceptions.ConnectionError as err:
                self.hostsinfo.remove((host, r_port, w_port))
            except Exception as err:
                print str(err)
        return []

    def remove(self, key):
        return "OK"

    def close(self):
        return True

PLUGIN_CLASS = Elliptics