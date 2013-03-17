from _abstractresulthandler import AbstractResultHandler

from combaine.plugins.DistributedStorage import DistributedStorageFactory

##
#
#
#   SHIT CODE: REWRITE!!!!
#
#3

import json
import collections
import httplib
import types
import itertools
import logging

agave_headers = {
        "User-Agent": "Yandex/Agave",
        "Keep-Alive": 300,
        "Connection": "Keep-Alive, TE",
        "TE": "deflate,gzip;q=0.3"
}

agave_hosts = json.load(open('/etc/combaine/combaine.json'))["cloud_config"]['agave_hosts']

logger = logging.getLogger("combaine")

class Agave(AbstractResultHandler):

    def __init__(self, **config):
        self.graph_name = config.get("graph_name")
        self.graph_template = config.get("graph_template")
        self.fields = config.get("Fields")
        self.template_dict = { "template" : self.graph_template,
                          "title"    : self.graph_name,
                          "graphname": self.graph_name
                        }

    def __makeUrls(self, frmt_dict):
        self.template_dict.update(frmt_dict)
        template = "/api/update/%(group)s/%(graphname)s?values=%(values)s&ts=%(time)i&template=%(template)s&title=%(title)s" % self.template_dict
        self.__send_point(template)

    def __send_point(self, url):
        for agv_host in agave_hosts:
            conn = httplib.HTTPConnection(agv_host, timeout=0.5)
            headers = agave_headers
            headers['Host'] = agv_host+':80'
            try:
                conn.request("GET", url, None, headers)
                print url, agv_host, conn.getresponse().read().splitlines()[0]
                logger.debug("%s %s" % (url, agv_host))
            except Exception as err:
                logger.debug("Unable to connect to one agave")


    def send(self, data):
        for_send = collections.defaultdict(list)
        time = None
        print data
        for items in data:
            for item in items:
                for sbg_name, val in item["values"].iteritems():
                    _sbg = sbg_name if sbg_name == item["groupname"] else (item["groupname"] + "-" + sbg_name)
                    if isinstance(val, types.ListType):
                        l = itertools.izip(self.fields[item['aggname'].split("@")[-1]],val)
                        _value = "+".join(("%s:%i" % x for x in l))
                    else:
                        _value = "%s:%s" % (item['aggname'].split("@")[-1], str(val) )
                    for_send[_sbg].append(_value)
                    time = item['time']
        for name, val in for_send.iteritems():
            frmt_dict = { "group"   : name,
                          "values"  : "+".join(val),
                          "time"    : time
            }
            self.__makeUrls(frmt_dict)



PLUGIN_CLASS = Agave
