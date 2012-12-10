
from abc import ABCMeta, abstractmethod

class AbstractFetcher(object):
    
    __metaclass__ = ABCMeta

    filter = None

    @abstractmethod
    def getData(self):
        """ Must return a generator object """
        raise Exception

