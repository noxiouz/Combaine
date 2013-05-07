class BasePluginException(Exception):
    pass

class NoPluginError(BasePluginException):

    def __init__(self, msg):
        self._msg = msg

    def __str__(self):
        return "NoPluginError: %s" % self._msg

    def __repr__(self):
        return self.__str__()


def load_plugin(plugin_name, name):
    print plugin_name
    try:
        module = __import__(name + "." + plugin_name, globals(), locals(), [], -1)
        print module
        return module.PLUGIN_CLASS
    except ImportError as err:
        raise NoPluginError(str(err))
    except AttributeError as err:
        raise Exception(str(err))
    else:
        return constructor