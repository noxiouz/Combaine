from abc import ABCMeta, abstractmethod

class AbstractResultHandler(object):

    __meta__ = ABCMeta

    @abstractmethod
    def handle(self, data):
        pass
