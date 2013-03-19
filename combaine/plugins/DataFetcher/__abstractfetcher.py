
from abc import ABCMeta, abstractmethod

class AbstractFetcher(object):
    
    __metaclass__ = ABCMeta

    filter = lambda x: True

    @abstractmethod
    def getData(self):
        """ Must return a generator object """
        raise Exception

