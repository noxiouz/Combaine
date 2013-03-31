
import json
import logging
import logging.handlers

__all__ = ["ParsingLogger", "AggregateLogger"]

def _initLogger(name):
    try:
        config = json.load(open('/etc/combaine/combaine.json'))['cloud_config']
    except Exception as err:
        pass
    else:
        _format = logging.Formatter("%(levelname)-5s %(asctime)s %(id)s %(message)s", "%Y-%m-%d %H:%M:%S")
        parsing_log = logging.getLogger('combaine.%s' % name)
        log_level = eval('logging.' + config['log_level'])

        fh = logging.handlers.TimedRotatingFileHandler('/var/log/combaine/%s.log' % name, when="midnight", backupCount=3)
        fh.setFormatter(_format)
        fh.setLevel(log_level)

        sh = logging.StreamHandler()
        sh.setFormatter(_format)
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
        return logging.LoggerAdapter(logging.getLogger("combaine.parsing"), {"id" : _id})

class AggregateLogger(object):

    def __new__(cls, _id):
        if not hasattr(cls, "_instanse"):
            cls._instanse = super(AggregateLogger, cls).__new__(cls)
            _initLogger("aggregate")
        GlobalLogId(_id)
        return logging.LoggerAdapter(logging.getLogger("combaine.aggregate"), {"id" : _id})

class DataFetcherLogger(object):

    def __new__(cls):
        if not hasattr(cls, "_instanse"):
            cls._instanse = super(DataFetcherLogger, cls).__new__(cls)
            _initLogger("datafetcher")
        return logging.LoggerAdapter(logging.getLogger("combaine.datafetcher"), {"id" : GlobalLogId.get_id()})

class DataGridLogger(object):

    def __new__(cls):
        if not hasattr(cls, "_instanse"):
            cls._instanse = super(DataGridLogger, cls).__new__(cls)
            _initLogger("datagrid")
        return logging.LoggerAdapter(logging.getLogger("combaine.datagrid"), {"id" : GlobalLogId.get_id()})

class CommonLogger(object):

    def __new__(cls):
        if hasattr(ParsingLogger, "_instanse"):
            return logging.LoggerAdapter(logging.getLogger("combaine.parsing"), {"id" : GlobalLogId.get_id()})
        elif hasattr(AggregateLogger, "_instanse"):
            return logging.LoggerAdapter(logging.getLogger("combaine.aggregate"), {"id" : GlobalLogId.get_id()})

