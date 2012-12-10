from abc import ABCMeta, abstractmethod

class BaseStorage():

    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self, **config):
        raise Exception

    @abstractmethod
    def put(self, abspath):
        raise Exception

    @abstractmethod
    def get(self, abspath):
        raise Exception

    @abstractmethod
    def delete(self, abspath):
        raise Exception

    @abstractmethod
    def modify(self, abspath):
        raise Exception

    @abstractmethod
    def list(self, abspath):
        raise Exception

    @abstractmethod
    def destroy(self):
        raise Exception

