
import json
import logging


def loadParsingConfig(config_name):
    try:
        path = '/etc/combaine/parsing/%s.json' % config_name
        parsing_config = json.load(open(path, 'r'))
    except Exception, err:
        log = getLogger('combaine')
        log.error('Error with parsing config %s: %s' % (config_name, str(err)))
        print str(err)
        return None
    else:
        return parsing_config


def loadCloudConfig():
    try:
        path = '/etc/combaine/combaine.json'
        cloud_config = json.load(open(path, 'r'))['cloud_config']
	#print cloud_config
    except Exception as err:
        #log = getLogger('combaine')
        #log.error('Error with combaine.json config: %s' %  str(err))
        print str(err)
        raise Exception(str(err)+"Error")
        return None
    else:
        return cloud_config

#def initLogger(**config):
try:
    config = json.load(open('/etc/combaine/combaine.json'))['cloud_config']
except Exception as err:
    pass
else:
    _format = logging.Formatter("%(levelname)-10s %(asctime)s %(message)s")
    app_log = logging.getLogger('combaine')
    log_level = eval('logging.' + config['log_level'])
    crit_handler = logging.FileHandler('/var/log/combaine/cloud.log')
    #crit_handler = logging.StreamHandler(sys.stdout)
    crit_handler.setFormatter(_format)
    crit_handler.setLevel(log_level)
    app_log.addHandler(crit_handler)
    app_log.setLevel(log_level)


