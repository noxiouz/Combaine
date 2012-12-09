from abc import ABCMeta, abstractmethod


class AbstractDistributedStorage(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def connect(self, namespace):
        raise NotImplementedError

    @abstractmethod
    def close(self):
        raise NotImplementedError

    @abstractmethod
    def insert(self):
        raise NotImplementedError

