from pprint import pprint
import re

from _abstractresulthandler import AbstractResultHandler
from combaine.common.loggers import CommonLogger

pattern = re.compile(r"\${([^}]*)}")


class UnsafelyCodeError(Exception):

    def __init__(self, msg):
        self._msg = msg

    def __str__(self):
        return self._msg

    def __repr__(self):
        return self.__str__()

def make_eval_string_safe(inp):
    """ As I use eval (so shit) - try make string safe. Replace in future """
    if ("import" in inp or "os" in inp):
        raise UnsafelyCodeError("Found unsafelly code in expresion: %s" % inp)
    out = inp.replace(" ","")
    return out

def coroutine(func):
    def start(*args, **kwargs):
        g = func(*args, **kwargs)
        return g
    return start

@coroutine
def make_template_placeholders(inp_aggresults):
    """ Make from aggresults:
    '20x': <dictionary-itemiterator object at 0x27c4998>,
    '30x': <dictionary-itemiterator object at 0x27c4ba8>}
    """
    subgroups = [name for name, value in inp_aggresults[0].values]
    out_put = dict(((_.aggname, _.values) for _ in inp_aggresults))
    try:
        for subgroup in subgroups:
            #('subgroupname', {'30x': 46.800000000000004, '20x': 741.20000000000005})
            yield subgroup, dict((key, next(out_put[key])[1]) for key in out_put.keys())
    except StopIteration:
        print "Stop"


class MathExp(AbstractResultHandler):

    def __init__(self, **config):
        self.logger = CommonLogger()
        try:
            self.name = config['name']
            self.expression = make_eval_string_safe(config["expression"])
            self.senders = config.get("send", [])
            self.logger.info("Evaluation expression: %s" % self.expression)
        except UnsafelyCodeError as err:
            self.logger.error(str(err))
            raise
        except KeyError as err:
            self.logger.error("MathExp. Missing config parametr: %s" % str(err))
            raise
        self._aggs = pattern.findall(self.expression)

    def handle(self, data):
        interest_results = [_ for _ in data if _.aggname in self._aggs]
        returned_result = dict()
        for subgroup_name, subgroup_data in make_template_placeholders(interest_results):
            code = self.expression
            for key, value in subgroup_data.iteritems():
                #re.subn(r"\${20x}","A","(${20x}+${30x})")
                code, n = re.subn(r"\${%s}" % key, str(value), code)
            self.logger.debug("After substitution %s" % code)
            try:
                res = eval(code)
                self.logger.info("MathExp: Result %s %s" % (subgroup_name, res))
                returned_result[subgroup_name] = res
            except Exception as err:
                self.logger.error("Exception in evaluation %s: %s" % (code, err))
        return returned_result

PLUGIN_CLASS = MathExp