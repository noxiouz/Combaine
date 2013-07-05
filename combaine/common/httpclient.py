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
        asCli = AsyncHTTPClient(self.io_loop)

        for label, url in urls.iteritems():
            self._counter += 1
            asCli.fetch(url, partial(self.callback, label), request_timeout=kwargs.get("timeout", 1))

        self.io_loop.start()
        return self._buffer

    def callback(self, label, response):
        self._counter -= 1
        if self._counter <= 0:
            self.io_loop.stop()
        if response.body is not None:
            self._buffer[label] = response

    def fetch_any(self, urls,**kwargs):
        self._buffer = dict()
        self._counter = 0
        asCli = AsyncHTTPClient(self.io_loop)
        for label, url in urls.iteritems():
            print url
            self._counter += 1
            asCli.fetch(url, partial(self.any_callback, label), request_timeout=kwargs.get("timeout", 1))
        self.io_loop.start()
        return self._buffer

    def any_callback(self, label, response):
        self._counter -= 1
        if not response.error:
            self._buffer[label] = response
            self.io_loop.stop()
        elif self._counter <= 0:
            self.io_loop.stop()


HTTPReq = HTTPRequest

if __name__ == "__main__":
    A = AsyncHTTP()
    print "Start"
    print A.fetch({ "A" : "http://www.yandex.ru", "B" : "http://www.google.com"}).keys()
    print A.fetch_any({ "A" : "http://www.yandexwfw.ru", "B" : "http://www.google.com"}).keys()
    print "DONE"
