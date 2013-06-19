import subprocess
import re
import itertools

from _abstractsender import AbstractSender

from combaine.common.loggers import CommonLogger
from combaine.common.configloader import parse_common_cfg

STATUSES = { "OK" : 0,
             "INFO" : 3,
             "WARNING" : 1,
             "CRITICAL" : 2
            }



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

class Juggler(AbstractSender):
    """
    type: juggler
    INFO: ["${50and20x}>-100", "${20x}<0"]
    WARNING: ["${50and20x}>1", "${50and20x}<0"]
    CRITICAL: ["${50and20x}>10", "${50and20x}<0"]
    OK: ["${50and20x}>10", "${50and20x}<0"]
    """

    pattern = re.compile(r"\${([^}]*)}")

    def __init__(self, **cfg):
        self.logger = CommonLogger()
        self._INFO = cfg.get("INFO", [])
        self._WARNING = cfg.get("WARNING", [])
        self._CRITICAL = cfg.get("CRITICAL", [])
        self.service = cfg["service"]
        self.description = cfg.get("description", "no description")
        self._OK = cfg.get("OK", [])
        self._aggs = list()
        for item in itertools.chain(self._INFO, self._WARNING, self._CRITICAL, self._OK):
            self._aggs += self.pattern.findall(item)
        self._aggs = list(set(self._aggs))
        self.logger.info(str(self._aggs))

    def _handling_one_expression(self, level, data, name):
        for expression in level:
                code = expression
                for key, value in data.iteritems():
                    code, n = re.subn(r"\${%s}" % key, str(value), code)
                self.logger.debug("After substitution %s in %s" % (code, name))
                res = eval(code)
                self.logger.debug("evalution result %s" % res)
                if res:
                    cmd = ("juggler_queue_event", "--host=" + name, "-n", self.service, "-s", "1232415", "-d", self.description)
                    try:
                        self.logger.debug(' '.join(cmd))
                    except subprocess.CalledProcessError as err:
                        self.logger.error("Calling juggler client was failed")

    def send(self, data):
        interest_results = filter(lambda x: x.aggname in self._aggs, data)
        self.logger.error("Interest: %s" % str(interest_results))
        for subgroup_name, subgroup_data in make_template_placeholders(interest_results):
            self._handling_one_expression(self._INFO, subgroup_data, subgroup_name)
            self._handling_one_expression(self._WARNING, subgroup_data, subgroup_name)
            self._handling_one_expression(self._CRITICAL, subgroup_data, subgroup_name)
           
PLUGIN_CLASS = Juggler

