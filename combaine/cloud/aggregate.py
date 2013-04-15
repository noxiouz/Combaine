#!/usr/bin/env python
import time
import logging
import urllib
import pprint
import collections
import itertools
import json
import socket
import hashlib

from combaine.plugins.Aggregators import AggregatorFactory
from combaine.plugins.DistributedStorage import DistributedStorageFactory
from combaine.plugins.ResultHandler import ResultHandlerFactory
from combaine.common.configloader.parsingconfigurator import ParsingConfigurator
from combaine.common.loggers import AggregateLogger
from combaine.common.loggers import CommonLogger
from combaine.common.interfaces.aggresult import AggRes

try:
    http_hand_url = json.load(open('/etc/combaine/combaine.json'))['Combainer']['Main']['HTTP_HAND']
except Exception as err:
    logger.error(str(err))

def split_hosts_by_dc(subgroups):
    hosts = urllib.urlopen("%s%s?fields=root_datacenter_name,fqdn" % (http_hand_url, subgroups)).read()
    if hosts == 'No groups found':
        return []
    host_dict = collections.defaultdict(list)
    for item in hosts.splitlines():
        dc, host = item.split('\t')
        host_dict[dc].append(host)
    return host_dict

def Main(groupname, config_name, agg_config_name, previous_time, current_time):
    uuid = hashlib.md5("%s%s%s%i%i" %(groupname, config_name, agg_config_name, previous_time, current_time)).hexdigest()[:10]
    logger = AggregateLogger(uuid)
    logger.info("Start aggregation: %s %s %s %i-%i" % (groupname, config_name, agg_config_name, previous_time, current_time))

    conf = ParsingConfigurator(config_name, agg_config_name)

    ds = DistributedStorageFactory(**conf.ds) # Get Distributed storage  
    if ds is None:
        logger.error('Failed to init distributed storage like MongoRS')
        return 'failed'

    if not ds.connect('combaine_mid/%s' % config_name):
        logger.error('Cannot connect to distributed storage like MongoRS')
        return 'failed'

    res_handlers = [ResultHandlerFactory(**_cfg) for _cfg in conf.resulthadlers]

    aggs = dict((_agg.name, _agg) for _agg in (AggregatorFactory(**agg_config) for agg_config in conf.aggregators))

    hosts = split_hosts_by_dc(groupname)

    all_data = list()
    for sbgrp in hosts.values():
        data_by_subgrp = collections.defaultdict(list)
        for hst in sbgrp:
           _l = ((ds.read("%s;%s;%i;%i;%s" % (hst.replace('-','_').replace('.','_'),\
                                                config_name, previous_time, current_time, _agg),\
                                                cache=True), _agg) for _agg in aggs)
           [data_by_subgrp[_name].append(val) for val, _name in _l]

        all_data.append(dict(data_by_subgrp))


    res = []
    for key in aggs.iterkeys():
        l = [ _item[key] for _item in all_data if _item.has_key(key)]
        one_agg_result = AggRes(aggs[key].name, hosts.keys(), conf.metahost or groupname, agg_config_name)
        one_agg_result.store_result(next(aggs[key].aggregate_group(l)))
        res.append(one_agg_result)

    #==== Clean RS from sourse data for aggregation ====
    logger.info("Hadling data by result handlers")
    try:
        for _res_handler in res_handlers:
            _res_handler.send(res) 
    except Exception as err:
        logger.exception(err)
        
    ds.close()
    logger.info("Aggregation has finished successfully")
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
        logger = CommonLogger()
        logger.error("Wrong message %s" % message)
        return
    else:
        try:
            res = Main(group_name, config_name, agg_config_name, prev_time, cur_time)
        except Exception as err:
            res = 'failed;Error: %s' % err
        finally:
            io.write(';'.join((res, message, socket.gethostname())))
