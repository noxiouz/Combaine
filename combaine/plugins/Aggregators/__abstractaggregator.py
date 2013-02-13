import re
from abc import ABCMeta, abstractmethod


TYPES = ( "RAW", "PROCESSED" )

class AbstractAggregator(object):
    
    __metaclass__ = ABCMeta

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

    @abstractmethod
    def aggregate_group(self, data):
        raise Exception

class RawAbstractAggregator(AbstractAggregator):

    agg_type = TYPES.index("RAW")

    def __init__(self, *args):
        super(RawAbstractAggregator, self).__init__(*args)

    def set_datagrid_backend(self, weakref_to_dg):
        self.dg = weakref_to_dg


class ProcessedAbstractAggregator(AbstractAggregator):

    agg_type = TYPES.index("PROCESSED")

    def __init__(self, *args):
        super(ProcessedAbstractAggregator, self).__init__(*args)


    def set_data(self, data):
        self.data = data
