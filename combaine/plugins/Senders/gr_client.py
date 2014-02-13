import collections
import types
import itertools
import socket

from combaine.plugins.Senders._abstractsender import AbstractSender

from combaine.common.loggers import CommonLogger
from combaine.common.configloader import parse_common_cfg

class GrSenderClient(AbstractSender):
    """
    type: agave_client
    items: [20x, 30x, 50/20x]
    graph_name: http_ok
    graph_template: http_ok
    """
    def __init__(self, **config):
        self.logger = CommonLogger()
        self.graph_name = config.get("graph_name")
        self.graph_template = config.get("graph_template")
        self.group = config['parsing_conf']['groups'][0]
        self.project = config['parsing_conf'].get('project', 'lost')
        self.fields = config.get("Fields")
        self.items = config.get('items', [])
        self.logger.debug(self.group, self.graph_name, self.graph_template)
        try:
            self.prefix = parse_common_cfg('combaine')["cloud_config"]['graphite_prefix']
        except KeyError:
            self.prefix = "from_agave.lost.cluster"

    def data_filter(self, data):
        return [res for res in data if res.aggname in self.items]

    def send(self, data):
        data = self.data_filter(data)
        for_send = collections.defaultdict(list)
        for aggres in data:
            for sbg_name, val in aggres.values:
                _sbg = sbg_name if sbg_name == aggres.groupname else "-".join((aggres.groupname, sbg_name))
                if isinstance(val, types.ListType): # Quantile
                    l = itertools.izip(self.fields, val)
                    _value = list(l)
                    for_send[_sbg].extend(_value)
                else: # Simple single value
                    _value = (aggres.aggname, val)
                    for_send[_sbg].append(_value)
                time = aggres.time

        messages = []

        for vhost, val in for_send.iteritems():
            #Message format:
            # metric.name value timestamp\n
            #base_metric = '.'.join([self.prefix, self.group, vhost.replace('.', '_'), self.graph_name])
            base_metric = [self.prefix,
                           self.project,
                           self.group,
                           vhost.replace('.', '_'),
                           self.graph_name,
                          ]
            base_metric = '.'.join(filter(lambda x: x != None, base_metric))
            for graph, point in val:
                metric = '.'.join([base_metric, graph])
                message = ' '.join([metric, str(point), str(time)])
                messages.append(message)

        messages = '\n'.join(messages)

        print messages

        try:
            gr_conn = socket.create_connection(('localhost', 42000), 0.25)
            gr_conn.sendall(messages)
        except socket.error as e:
            self.logger.error('Error communicating graphite-sender: %s, %s' % (e.errno, e.strerror))
        else:
            gr_conn.close()

PLUGIN_CLASS = GrSenderClient
