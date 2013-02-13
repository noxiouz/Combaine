from __AbstractStorage import AbstractDistributedStorage

import requests
import msgpack

import string
import random

class Elliptics(AbstractDistributedStorage):

    def __init__(self, **config):
        cfg = [_i.split(":") for _i in config["proxy_hosts"]]
        random.shuffle(cfg)
        self.hostsinfo = cfg
        self.read_url = string.Template("http://${HOST}:${R_PORT}/get/${KEY}?ioflags=3072")
        self.write_url = string.Template("http://${HOST}:${W_PORT}/upload/${KEY}?ioflags=3072")

    def connect(self, namespace):
        return True

    def insert(self, key, data):
        for host, r_port, w_port in self.hostsinfo:
            try:
                r = requests.post(self.write_url.substitute(KEY=key, HOST=host, W_PORT=w_port), data=msgpack.packb(data), timeout=1)
                if r.status_code == 503: #because elliptics write cache bug
                    return True
            except requests.exceptions.Timeout as err:
                pass
            except requests.exceptions.ConnectionError as err:
                pass
        return False

    def read(self, key, cache=False):
        for host, r_port, w_port in self.hostsinfo:
            try:
                r = requests.post(self.read_url.substitute(KEY=key, HOST=host, R_PORT=r_port), timeout=1)
                if r.ok:
                    ret = list(msgpack.unpackb(r.content))
                    return ret
            except requests.exceptions.Timeout as err:
                pass
            except requests.exceptions.ConnectionError as err:
                pass
        return []

    def remove(self, key):
        return "OK"

    def close(self):
        return True

PLUGIN_CLASS = Elliptics
