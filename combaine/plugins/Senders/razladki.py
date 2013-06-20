import collections
import httplib
import types
import itertools

from _abstractsender import AbstractSender

from combaine.common.loggers import CommonLogger
from combaine.common.configloader import parse_common_cfg

agave_headers = {
        "User-Agent": "Yandex/Agave",
        "Connection": "TE",
        "TE": "deflate,gzip;q=0.3"
}

try:
    razladki_hosts = parse_common_cfg('combaine')["cloud_config"]['razladki_hosts']
except Exception as err:
    print err

class Razl(AbstractSender):
    """
    type: razladki
    items: [20x, 30x, 50/20x]
    """

    def __init__(self, **config):
        self.logger = CommonLogger()
        self.items = config.get('items', [])
        self.project = config['project']
        self.param = config['param']
#url = "http://" + config['razladki_host'] + "/save_new_datapoint/" + razladki_project
# + "?param=" + str(doc['group']) + '-' + graph + '_' + str(_temp_)  + 
#'&value=' + str(doc['data'][_temp_]) + '&host_group=' + str(doc['group']) + '&ts=' + str(doc['time'])
        self.url = "http://%s/save_new_datapoint/%s%s" % (razladki_hosts,
                                                             self.project,
                    "?param=%(param)s&value=%(value)s&host_group=%(hg)s&ts=%(time)d")
        self.logger.info("razladki init succsfully")

    def __send_point(self, url):
        for agv_host in agave_hosts:
            conn = httplib.HTTPConnection(agv_host, timeout=1)
            headers = agave_headers
            headers['Host'] = agv_host+':80'
            try:
                conn.request("GET", url, None, headers)
                _r = conn.getresponse()
                self.logger.info("%s %s %s %s %s" % (agv_host, _r.status, _r.reason, _r.read().strip('\r\n'), url))
            except Exception as err:
                self.logger.exception("Unable to connect to one agave")
            else:
                _r.close()

    def _make_url(self, **kwargs):
        url = self.url % kwargs
        print url

    def send(self, data):
        data = filter(lambda x: x.aggname in self.items, data)
        for aggres in data:
            for sbg_name, val in aggres.values:
                _sbg = sbg_name if sbg_name == aggres.groupname else "-".join((aggres.groupname, sbg_name))
                if isinstance(val, types.ListType): # Quantile
                    self.logger.warning("Unable to ListType to razladki")
                    pass
                else: # Simle single value
                    self._make_url(value=val, hg=_sbg, time=aggres.time, param=self.param)

PLUGIN_CLASS = Razl
