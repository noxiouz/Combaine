import httplib

import requests

from combaine.common.loggers import CommonLogger
from combaine.common.configloader import parse_common_cfg
from combaine.common.configloader import parse_misc_cfg

agave_headers = {
        "User-Agent": "Yandex/Agave",
        "Connection": "TE",
        "TE": "deflate,gzip;q=0.3"
}

try:
    agave_hosts = parse_common_cfg('combaine')["cloud_config"]['agave_hosts']
except Exception as err:
    print err

def Agave(group_name=None, **config):
    logger = CommonLogger()
    timeout = config.get('timeout', 1)
    group_name = config.get('group') or group_name
    graph_name = config.get("name")
    graph_template = config.get("template")
    title = config.get("template")
#"/api/update/%(group)s/%(graphname)s?values=%(values)s&ts=%(time)i&template=%(template)s&title=%(title)s"
    #"/api/update/%(group)s/%(graphname)s?template=%(template)s&title=%(title)s&values=%(values)s&ts=%(time)i"
    template = "/api/update/%s/%s?template=%s&title=%s&" % (group_name, graph_name, graph_template, title) 
    def wrapper(point_data, point_time):
        for host on agave_hosts:
            conn = httplib.HTTPConnection(host, timeout=timeout)
            headers = agave_headers
            headers['Host'] = host+':80'
            url = template + "values=%s&ts=%i" % (point_data, point_time)
            print url
            try:
                conn.request("GET", url, None, headers)
                #print url, agv_host, conn.getresponse().read().splitlines()[0]
                _r = conn.getresponse()
                logger.info("%s %s %s %s %s" % (agv_host, _r.status, _r.reason, _r.read().strip('\r\n'), url))
            except Exception as err:
                logger.exception("Unable to connect to one agave")
            else:
                _r.close()
    return wrapper



PLUGIN_CLASS = Agave
