
from abc import ABCMeta, abstractmethod

from combaine.common.loggers import DataGridLogger

class AbstractDataGrid(object):
    
    __metaclass__ = ABCMeta

    @abstractmethod
    def _preparePlace(self, info, index_field):
        """ info - all information about elementary data structure for create table. """ 
        raise Exception

    @abstractmethod
    def putData(self, data):
        """ data - iteratable structure of records. Such as streams in MySQL or Dict in Mongo"""
        raise Exception

    def tablename(self):
        return self.tablename

    @abstractmethod
    def perfomCustomQuery(self, query):
        raise Exception
