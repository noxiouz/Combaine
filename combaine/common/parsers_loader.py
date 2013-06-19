import os
import imp

__all__ = ["PARSERS"]

PATH = '/usr/lib/yandex/combaine/parsers'

def _isPlugin(candidate):
    name, extension = os.path.splitext(candidate)
    if name != "__init__" and extension in (".py", ".pyc", ".pyo"):
        return True
    else:
        return False

def _plugin_import():
    modules = set(map(lambda x: x.split('.')[0], filter(_isPlugin, os.listdir(PATH))))
    all_parser_functions = {}
    for module in modules:
        try:
            fp, path, descr = imp.find_module(module, [PATH])
        except ImportError as err:
            continue
        else:
            try:
                _temp = imp.load_module("temp", fp, path, descr)
                for item in filter(lambda x: not x.startswith("__"), dir(_temp)):
                    candidate = getattr(_temp, item)
                    if callable(candidate):
                        all_parser_functions[item] = candidate
            finally:
                if fp:
                    fp.close()
    return all_parser_functions

PARSERS = _plugin_import()

