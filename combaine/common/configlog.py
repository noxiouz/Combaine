import logging, sys, json
import logging.handlers

def init_logging(loggers):
    """
    ----------------------------------------------------------
    loggers - list of dictionaries. Keys are depended of type
    ----------------------------------------------------------
    EXAMPLE:
    For FILE type:
    {
        "type" : "FILE",
        "path" : "/var/log/combaine/combaine.log"
        "level": "INFO"
    }
    For STDOUT:
    {
        "type" : "STDOUT"
    }
    """
    _level = {
        "INFO"  :   logging.INFO,
        "DEBUG" :   logging.DEBUG,
        "ERROR" :   logging.ERROR,
    }
    try:
        POSSIBLE = ("FILE","STDOUT")
        _format = logging.Formatter("%(asctime)s %(levelname)-4s %(message)s")
        app_log = logging.getLogger('combaine')
        for logger in loggers:
            if logger.has_key("level"):
                lvl = _level[logger["level"]] if _level.has_key(logger["level"]) else logging.INFO
            else:
                lvl = logging.INFO
            if logger["type"] in POSSIBLE:
                if logger["type"] == "FILE":
                    lhandler = logging.handlers.TimedRotatingFileHandler(logger["path"], when="midnight", backupCount=3)
                    lhandler.setFormatter(_format)
                    lhandler.setLevel(lvl)
                    app_log.addHandler(lhandler)
                if logger["type"] == "STDOUT":
                    lhandler = logging.StreamHandler(sys.stdout)
                    lhandler.setFormatter(_format)
                    lhandler.setLevel(lvl)
                    app_log.addHandler(lhandler)
            app_log.setLevel(lvl)
            #app_log.info('Init logger successfully')
    except Exception, err:
        app_log.addHandler(logging.StreamHandler(sys.stdout))
        print "Exception in logger init: "+str(err)
        return False
    return True

init_logging((json.load(open('/etc/combaine/combaine.json','r'))["Combainer"]["logger"], ) )

class configChecker(object):

    def __init__(self, config_path):
        try:
            self.raw_config = json.load(open(config_path),r)
        except Exception, err:
            print str(err)
            raise

    def checkOption(self, path, assertFunc):
        option = self.raw_config
        try:
            for section in path.split('/'):
                try:
                    option = optinon[section]
                except KeyError, err:
                    print "Unvalid section %s" % str(err)
                    return False
        except Exception, err:
            print 'Some Error %s' % str(err)
            return False
        else:
            return assertFunc(option)

    def assertType(self, _class):
        def wrapper(option):
            return isinstance(option, _class)
        return wrapper

