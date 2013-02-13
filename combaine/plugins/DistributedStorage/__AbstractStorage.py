from abc import ABCMeta, abstractmethod


class AbstractDistributedStorage(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def connect(self, namespace):
        raise NotImplementedError

    @abstractmethod
    def close(self, namespace):
        raise NotImplementedError

    @abstractmethod
    def close(self):
        raise NotImplementedError

    @abstractmethod
    def insert(self, key, data):
        raise NotImplementedError

    @abstractmethod
    def read(self, key):
        raise NotImplementedError

    @abstractmethod
    def remove(self, key):
        raise NotImplementedError
