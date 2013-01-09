import re
from abc import ABCMeta, abstractmethod


class AbstractAggregator(object):
    
    def __init__(self):
        self.table_regex = re.compile('%TABLENAME%')
        self.time_regex = re.compile('%%')

    @abstractmethod
    def aggregate(self, datagrid_object):
        raise Exception

    @abstractmethod
    def _pack(self, data):
        pass

    @abstractmethod
    def _unpack(self, data):
        pass

