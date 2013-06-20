import time
from functools import partial

from tornado.ioloop import IOLoop
from tornado.httpclient import AsyncHTTPClient
from tornado.httpclient import HTTPRequest

__all__ = ["AsyncHTTP", "HTTPReq"]

class AsyncHTTP(object):
    """Simple wrapper over tornado.httpclient.AsyncHTTPClient"""

    def __init__(self):
        self._counter = 0
        self._globaltimeout = None
        self._buffer = None
        self.io_loop = IOLoop.instance()
    
    def fetch(self, urls, **kwargs):
        self._buffer = dict() 
        self._counter = 0  
        timeout = kwargs.get("timeout", 1)
        handler = self.io_loop.add_timeout(time.time() + timeout, self.io_loop.stop)

        for label, url in urls.iteritems():
            self._counter += 1
            asCli = AsyncHTTPClient(self.io_loop)
            asCli.fetch(url, partial(self.callback, handler, label))

        self.io_loop.start()
        return self._buffer


    def callback(self, timeout_handler, label, response):
        self._counter -= 1
        if self._counter <= 0:
            self.io_loop.remove_timeout(timeout_handler)
            self.io_loop.stop()
        self._buffer[label] = response

HTTPReq = HTTPRequest

if __name__ == "__main__":
    A = AsyncHTTP()
    print "Start"
    print A.fetch({ "A" : "http://www.yandex.ru", "B" : "http://www.google.com"})
    print "DONE"
