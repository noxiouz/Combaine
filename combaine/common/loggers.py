import logging
import logging.handlers

import combaine.common.configloader.config
from logging.handlers import SysLogHandler

from tornado.log import LogFormatter

__all__ = ["ParsingLogger", "AggregateLogger", "CommonLogger"]


class CombaineLogAdapter(logging.LoggerAdapter):

    def process(self, msg, kwargs):
        return '%s %s %s' % (self.extra['id'], self.extra['ident'], msg), kwargs

def _initLogger(name):
    try:
        config = combaine.common.configloader.config.parse_common_cfg("combaine")['cloud_config']
    except Exception as err:
        pass
        print err
    else:
        _format = logging.Formatter("%(name)s: %(levelname)-5s %(message)s")
        parsing_log = logging.getLogger('combaine.%s' % name)
        log_level = eval('logging.' + config['log_level'])

        #fh = SysLogHandler()
        fh = logging.handlers.TimedRotatingFileHandler('/var/log/combaine/%s.log' % name, when="midnight", backupCount=3)
        fh.setFormatter(_format)
        fh.setLevel(log_level)

        sh = logging.StreamHandler()
        #sh.setFormatter(_format)
        fmt = "%(ident)s %(levelname)-5s %(id)s %(message)s"
        sh.setFormatter(LogFormatter(fmt))
        sh.setLevel(log_level)
    
        parsing_log.addHandler(fh)
        parsing_log.addHandler(sh)
        parsing_log.setLevel(log_level)

class GlobalLogId(object):

    def __new__(cls, _id):
        if not hasattr(cls, "_instanse"):
            print "INIT GLOBAL LOGGER ID"
            cls._instanse = super(GlobalLogId, cls).__new__(cls)
        cls._id = _id

    @classmethod
    def get_id(cls):
        if hasattr(cls, "_id"):
            return cls._id
        else:
            return "DUMMY_ID"

class ParsingLogger(object):

    def __new__(cls, _id):
        if not hasattr(cls, "_instanse"):
            cls._instanse = super(ParsingLogger, cls).__new__(cls)
            _initLogger("parsing")
        GlobalLogId(_id)
        return CombaineLogAdapter(logging.getLogger("combaine.parsing"), {"id": _id, "ident": "combaine/parsing"})

class AggregateLogger(object):

    def __new__(cls, _id):
        if not hasattr(cls, "_instanse"):
            cls._instanse = super(AggregateLogger, cls).__new__(cls)
            _initLogger("aggregate")
        GlobalLogId(_id)
        return CombaineLogAdapter(logging.getLogger("combaine.aggregate"), {"id" : _id, "ident": "combaine/aggregate"})

class DataFetcherLogger(object):

    def __new__(cls):
        if not hasattr(cls, "_instanse"):
            cls._instanse = super(DataFetcherLogger, cls).__new__(cls)
            _initLogger("datafetcher")
        return CombaineLogAdapter(logging.getLogger("combaine.datafetcher"), {"id" : GlobalLogId.get_id(), "ident": "combaine/parsing"})

class DataGridLogger(object):

    def __new__(cls):
        if not hasattr(cls, "_instanse"):
            cls._instanse = super(DataGridLogger, cls).__new__(cls)
            _initLogger("datagrid")
        return CombaineLogAdapter(logging.getLogger("combaine.datagrid"), {"id" : GlobalLogId.get_id(), "ident": "combaine/parsing"})

class CommonLogger(object):

    def __new__(cls):
        if hasattr(ParsingLogger, "_instanse"):
            return CombaineLogAdapter(logging.getLogger("combaine.parsing"), {"id" : GlobalLogId.get_id(), "ident": "combaine/parsing"})
        elif hasattr(AggregateLogger, "_instanse"):
            return CombaineLogAdapter(logging.getLogger("combaine.aggregate"), {"id" : GlobalLogId.get_id(), "ident": "combaine/aggregate"})
        else:
            return CombaineLogAdapter(logging.getLogger("combaine"), {"id" : GlobalLogId.get_id(), "ident": "combaine"})

