
from abc import ABCMeta, abstractmethod

def def_filter(*args):
    return True

class AbstractFetcher(object):
    
    __metaclass__ = ABCMeta

    filter = def_filter

    @abstractmethod
    def getData(self):
        """ Must return a generator object """
        raise Exception

