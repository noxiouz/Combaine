import collections
import httplib
import types
import itertools
import logging

from _abstractresulthandler import AbstractResultHandler

from combaine.plugins.DistributedStorage import DistributedStorageFactory
from combaine.common.loggers import CommonLogger
from combaine.common.configloader import parse_common_cfg

##
#
#
#   SHIT CODE: REWRITE!!!!
#
#3

agave_headers = {
        "User-Agent": "Yandex/Agave",
        "Connection": "TE",
        "TE": "deflate,gzip;q=0.3"
}

try:
    agave_hosts = parse_common_cfg('combaine')["cloud_config"]['agave_hosts']
except Exception as err:
    print err

class Agave(AbstractResultHandler):

    def __init__(self, **config):
        self.logger = CommonLogger()
        self.graph_name = config.get("graph_name")
        self.graph_template = config.get("graph_template")
        self.fields = config.get("Fields")
        self.template_dict = {  "template" : self.graph_template,
                                "title"    : self.graph_name,
                                "graphname": self.graph_name
                        }
        self.logger.debug(self.template_dict)

    def __makeUrls(self, frmt_dict):
        self.template_dict.update(frmt_dict)
        template = "/api/update/%(group)s/%(graphname)s?values=%(values)s&ts=%(time)i&template=%(template)s&title=%(title)s" % self.template_dict
        self.__send_point(template)

    def __send_point(self, url):
        for agv_host in agave_hosts:
            conn = httplib.HTTPConnection(agv_host, timeout=1)
            headers = agave_headers
            headers['Host'] = agv_host+':80'
            try:
                conn.request("GET", url, None, headers)
                #print url, agv_host, conn.getresponse().read().splitlines()[0]
                _r = conn.getresponse()
                self.logger.info("%s %s %s %s %s" % (agv_host, _r.status, _r.reason, _r.read().strip('\r\n'), url))
            except Exception as err:
                self.logger.exception("Unable to connect to one agave")
            else:
                _r.close()

    def send(self, data):
        for_send = collections.defaultdict(list)
        for aggres in data:
            for sbg_name, val in aggres.values:
                _sbg = sbg_name if sbg_name == aggres.groupname else "-".join((aggres.groupname, sbg_name))
                if isinstance(val, types.ListType): # Quantile
                    l = itertools.izip(self.fields[aggres.aggname], val)
                    _value = "+".join(("%s:%s" % x for x in l))
                else: # Simle single value
                    _value = "%s:%s" % (aggres.aggname, val)
                for_send[_sbg].append(_value)
                time = aggres.time

        for name, val in for_send.iteritems():
            frmt_dict = { "group"   : name,
                          "values"  : "+".join(val),
                          "time"    : time
            }
            self.__makeUrls(frmt_dict)


PLUGIN_CLASS = Agave
