#! /usr/bin/env python
import sys
sys.path.append("/home/noxiouz/git_repo/github/Combaine")
print sys.path

from combaine.plugins.Aggregators import AggregatorFactory
from combaine.plugins.DistributedStorage import DistributedStorageFactory
from combaine.common.configloader.parsingconfigurator import ParsingConfigurator
import config

import time
import logging
import urllib
import pprint
import collections
import json

logger = logging.getLogger("combaine")
#DIRTY HACK
http_hand_url = json.load(open('/etc/combaine/combaine.json'))['Combainer']['Main']['HTTP_HAND']

def split_hosts_by_subgroups(hosts):
    return [set(hosts[:5]),]

def split_hosts_by_dc(subgroups):
    hosts = urllib.urlopen("%s%s?fields=root_datacenter_name,fqdn" % (http_hand_url, subgroups)).read()
    print hosts
    if hosts == 'No groups found':
        print "Ilegal group"
        return []
    host_dict = collections.defaultdict(list)
    for item in hosts.splitlines():
        dc, host = item.split('\t')
        host_dict[dc].append(host)
    return host_dict.values()


def Main(groupname, config_name, agg_config_name, previous_time, current_time):
    #print "===== INITIALIZTION ====" 
    conf = ParsingConfigurator(config_name, agg_config_name)
    #print "===== DS init =========="
    ds = DistributedStorageFactory(**conf.ds) # Get Distributed storage  
    if ds is None:
        logger.error('%s Failed to init distributed storage like MongoRS' % uuid)
        return 'failed'
    if not ds.connect('test_combaine_mid/%s' % conf.parser.replace(".", "_").replace("-","_")): # CHECK NAME OF COLLECTION!!!!
        logger.error('%s Cannot connect to distributed storage like MongoRS' % uuid)
        return 'failed'
    aggs = dict((_agg.name, _agg) for _agg in (AggregatorFactory(**agg_config) for agg_config in conf.aggregators))
    #print "====== GET HOSTS LIST ===="
    hosts = split_hosts_by_dc(groupname)

    all_data = list()
    for sbgrp in hosts:
        data_by_subgrp = collections.defaultdict(list)
        for hst in sbgrp:
            [data_by_subgrp[_agg].append(\
                                            ds.read("%s;%i;%i;%s" % (hst.replace('-','_').replace('.','_'), previous_time, current_time, _agg))\
                                        ) for _agg in aggs]
        all_data.append(dict(data_by_subgrp))

    for key in aggs.iterkeys():
        print key
        l = [ _item[key] for _item in all_data]
        print [i for i in aggs[key].aggregate_group(l)]

