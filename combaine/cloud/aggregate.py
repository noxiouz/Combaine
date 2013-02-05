#! /usr/bin/env python

from combaine.plugins.Aggregators import AggregatorFactory
from combaine.plugins.DistributedStorage import DistributedStorageFactory
from combaine.plugins.ResultHandler import ResultHandlerFactory
from combaine.common.configloader.parsingconfigurator import ParsingConfigurator
import config

import time
import logging
import urllib
import pprint
import collections
import itertools
import json
import socket
import hashlib

logger = logging.getLogger("combaine")

try:
    http_hand_url = json.load(open('/etc/combaine/combaine.json'))['Combainer']['Main']['HTTP_HAND']
except Exception as err:
    logger.error(str(err))

def split_hosts_by_dc(subgroups):
    hosts = urllib.urlopen("%s%s?fields=root_datacenter_name,fqdn" % (http_hand_url, subgroups)).read()
    if hosts == 'No groups found':
        print "Ilegal group"
        return []
    host_dict = collections.defaultdict(list)
    for item in hosts.splitlines():
        dc, host = item.split('\t')
        host_dict[dc].append(host)
    return host_dict

def formatter(aggname, subgroupsnames, groupname, aggconfig):
    def wrap(resitem):
        res = dict()
        res['time'], values = resitem.items()[0]
        l = itertools.izip_longest(subgroupsnames, values, fillvalue=groupname)
        res['values'] = dict((x for x in l))
        res['aggname'] = aggname
        res['aggconfigname'] = aggconfig
        res['groupname'] = groupname
        return res
    return wrap

def Main(groupname, config_name, agg_config_name, previous_time, current_time):
    uuid = hashlib.md5("%s%s%s%i%i" %(groupname, config_name, agg_config_name, previous_time, current_time)).hexdigest()
    logger.info("Start aggregation: %s %s %s %s %i-%i" % (uuid, groupname, config_name, agg_config_name, previous_time, current_time))
    print "===== INITIALIZTION ====" 
    conf = ParsingConfigurator(config_name, agg_config_name)

    print "===== DS init =========="
    ds = DistributedStorageFactory(**conf.ds) # Get Distributed storage  
    if ds is None:
        logger.error('%s Failed to init distributed storage like MongoRS' % uuid)
        return 'failed'
    if not ds.connect('combaine_mid/%s' % config_name): # CHECK NAME OF COLLECTION!!!!
        logger.error('%s Cannot connect to distributed storage like MongoRS' % uuid)
        return 'failed'
    res_handlers = [ ResultHandlerFactory(**_cfg) for _cfg in conf.resulthadlers]

    aggs = dict((_agg.name, _agg) for _agg in (AggregatorFactory(**agg_config) for agg_config in conf.aggregators))
    print "====== GET HOSTS LIST ===="
    hosts = split_hosts_by_dc(groupname)

    all_data = list()
    for sbgrp in hosts.values():
        data_by_subgrp = collections.defaultdict(list)
        for hst in sbgrp:
           # [data_by_subgrp[_agg].append(\
           #     ds.read("%s;%i;%i;%s" % (hst.replace('-','_').replace('.','_'), previous_time, current_time, _agg), cache=True)\
           #                             ) for _agg in aggs]
           _l = ((ds.read("%s;%s;%i;%i;%s" % (hst.replace('-','_').replace('.','_'), config_name, previous_time, current_time, _agg), cache=True), _agg) for _agg in aggs)
           [data_by_subgrp[_name].append(val) for val, _name in _l]

        all_data.append(dict(data_by_subgrp))
    res = []
    for key in aggs.iterkeys():
        l = [ _item[key] for _item in all_data if _item.has_key(key)]
        f = formatter(aggs[key].name, hosts.keys(), groupname, agg_config_name)
        res.append(map(f,(i for i in aggs[key].aggregate_group(l) if i is not None)))
    #==== Clean RS from sourse data for aggregation ====
    [_res_handler.send(res) for _res_handler in res_handlers]
    map(ds.remove, ds.cache_key_list)
    #print ds.clear_namespace()
    ds.close()
    logger.info("%s Success" % uuid)
    return "Success"

def aggregate_group(io):
    """Cloud wrapper """
    message = ""
    try:
        message = io.read()
        group_name, config_name, agg_config_name, prev_time, cur_time = message.split(';')
        prev_time = int(prev_time)
        cur_time = int(cur_time)
    except Exception as err:
        io.write("failed;Wrong message format:%s;%s;%s" % (message, socket.gethostname(), str(err)))
        logger.error("Wrong message %s" % message)
        return
    else:
        try:
            res = Main(group_name, config_name, agg_config_name, prev_time, cur_time)
        except Exception as err:
            res = 'failed;Error: %s' % err
            logger.error(str(err), exc_info=1)
        finally:
            logger.info("For %s: %s" % (message, str(res)))
            io.write(';'.join((res, message, socket.gethostname())))
