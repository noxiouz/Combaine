

def AggregatorFactory(**config):
    try:
        plugin_name = config['type']
        module = __import__(plugin_name, globals(), locals(), [], -1)
        constructor = module.PLUGIN_CLASS(**config)
    except KeyError, err:
        print 'Key error %s' % err
        return None
    except Exception, err:
        print str(err)
        return None
    else:
        return constructor

