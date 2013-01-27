from abc import ABCMeta, abstractmethod

class AbstractResultHandler(object):

    __meta__ = ABCMeta

    @abstractmethod
    def send(self, data):
        pass
