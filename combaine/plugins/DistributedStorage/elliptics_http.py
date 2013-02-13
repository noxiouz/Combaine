from __AbstractStorage import AbstractDistributedStorage

import requests
import msgpack

import string

class Elliptics(AbstractDistributedStorage):

    def __init__(self, **config):
        self.host = config["proxy_hosts"]
        self.write_port = config.get('write_port', 8080)
        self.read_port = config.get('read_port', 80)
        self.packer = msgpack.Packer()
        self.unpacker = msgpack.Unpacker()
        self.read_url = string.Template("http://%s:%i/get/${KEY}?ioflags=3072" % (self.host, self.read_port))
        self.write_url = string.Template("http://%s:%i/upload/${KEY}?ioflags=3072" % (self.host, self.write_port))

    def connect(self, namespace):
        return True

    def insert(self, key, data):
        r = requests.post(self.write_url.substitute(KEY=key), data=msgpack.packb(data))
        return True

    def read(self, key, cache=False):
        r = requests.post(self.read_url.substitute(KEY=key))
        if r.ok:
            return list(msgpack.unpackb(r.content))
        else:
            return []

    def remove(self, key):
        return "OK"

    def close(self):
        return True

PLUGIN_CLASS = Elliptics
