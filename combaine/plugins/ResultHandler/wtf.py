import operator

from _abstractresulthandler import AbstractResultHandler
from combaine.common.loggers import CommonLogger

BIND = { "+" : operator.add,
         "-" : operator.sub,
         "*" : operator.mul,
         "/" : operator.truediv,
}

class Aggregator(AbstractResultHandler):

    def __init__(self, **config):
        try:
            self._construct_lambda(config["value"])
        except Exception as err:
            print err

    def _construct_lambda(self, expression):
        pass


    def send(self, data):
        pass

PLUGIN_CLASS = Aggregator
