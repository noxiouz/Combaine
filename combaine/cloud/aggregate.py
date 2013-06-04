#!/usr/bin/env python
import time
import logging
import urllib
import pprint
import collections
import itertools
import socket
import hashlib

from combaine.plugins.Aggregators import AggregatorFactory
from combaine.plugins.DistributedStorage import DistributedStorageFactory
from combaine.plugins.ResultHandler import ResultHandlerFactory
from combaine.plugins.Senders import SenderFactory
from combaine.common.configloader.parsingconfigurator import ParsingConfigurator
from combaine.common.loggers import AggregateLogger
from combaine.common.loggers import CommonLogger
from combaine.common.interfaces.aggresult import AggRes
from combaine.common.interfaces.aggresult import HandlerRes
from combaine.common.configloader import parse_common_cfg

try:
    http_hand_url = parse_common_cfg("combaine")['Combainer']['Main']['HTTP_HAND']
except Exception as err:
    print err
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

    res_handlers = [item for item in (ResultHandlerFactory(**_cfg) for _cfg in conf.resulthadlers) if item is not None]

    res_senders = [item for item in (SenderFactory(**_cfg) for _cfg in conf.senders) if item is not None]

    aggs = dict((_agg.name, _agg) for _agg in (AggregatorFactory(**agg_config)\
                         for agg_config in conf.aggregators) if _agg is not None)

    hosts = split_hosts_by_dc(groupname)

    all_data = list()
    for sbgrp in hosts.values():
        data_by_subgrp = collections.defaultdict(list)
        for hst in sbgrp:
           _l = ((ds.read("%s;%s;%i;%i;%s" % (hst.replace('-','_').replace('.','_'),\
                                                config_name, previous_time, current_time, _agg)\
                                            ), _agg) for _agg in aggs)
           [data_by_subgrp[_name].append(val) for val, _name in _l]

        all_data.append(dict(data_by_subgrp))


    res = []
    for key in aggs.iterkeys():
        l = [ _item[key] for _item in all_data if _item.has_key(key)]
        one_agg_result = AggRes(aggs[key].name, hosts.keys(), conf.metahost or groupname, agg_config_name)
        one_agg_result.store_result(next(aggs[key].aggregate_group(l)))
        res.append(one_agg_result)

    logger.info("Hadling data by result handlers")
    print res_handlers
    handler_results = list()
    try:
        for _res_handler in res_handlers:
            handler_result = HandlerRes(_res_handler.name, hosts.keys(), conf.metahost or groupname, agg_config_name)
            handler_result.store_result(_res_handler.handle(res), previous_time)
            handler_results.append(handler_result)
    except Exception as err:
        logger.exception(err)
    res.extend(handler_results)

    logger.info("Hadling data by senders")
    try:
        for _res_sender in res_senders:
            _res_sender.send(res) 
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
