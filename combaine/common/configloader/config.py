import yaml
import json
import os
from functools import partial

from combaine.common.loggers import CommonLogger
from combaine.common import constants

__all__ = ["FormatError", "MissingConfigError", "parse_agg_cfg", "parse_parsing_cfg", "parse_common_cfg"]


class ConfigError(Exception):
    pass

class FormatError(ConfigError):

    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
        return "Invalid file format %s. Only JSON and YAML are allowed." % self.msg

    def __str__(self):
        return self.__repr__()


class MissingConfigError(ConfigError):

    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
        return "Missing config file: %s.(%s)" % (self.msg, '|'.join(constants.VALID_CONFIG_EXTENSIONS))

    def __str__(self):
        return self.__repr__()


def _handle_json(data):
    try:
        return json.loads(data)
    except ValueError as err:
        return None

def _handle_yaml(data):
    try:
        return yaml.load(data)
    except yaml.YAMLError as err:
        return None


def _combaine_config(path, name):
    path = path.rstrip('/')
    L = CommonLogger()
    cfg = [_cfg for _cfg in ("%s/%s.%s" % (path, name, ext) for ext in constants.VALID_CONFIG_EXTENSIONS) if os.path.isfile(_cfg)]
    if len(cfg) == 0:
        raise MissingConfigError("%s/%s" % (path, name))
    elif len(cfg) > 1:
        L.debug("More than one config with name %s. Use %s" % (name, cfg[0]))

    with open(cfg[0]) as f:
        _data = f.read()
    data = _handle_yaml(_data) or _handle_json(_data)
    if data is None:
        raise FormatError("%s/%s" % (path, name))
    else: 
        return data
    
parse_common_cfg = partial(_combaine_config, constants.COMMON_PATH)
parse_agg_cfg = partial(_combaine_config, constants.AGG_PATH)
parse_parsing_cfg = partial(_combaine_config, constants.PARS_PATH)
parse_misc_cfg = partial(_combaine_config, constants.MISC_PATH)

if __name__ == "__main__":
    print parse_agg_cfg("http_ok")
    print parse_parsing_cfg("photo_proxy")
    print parse_common_cfg("combaine")

