#! /usr/bin/env python

from combaine.plugins.DataGrid import DataGridFactory
from combaine.plugins.DataFetcher import FetcherFactory
from combaine.plugins.Aggregators import AggregatorFactory
from combaine.plugins.DistributedStorage import DistributedStorageFactory
from combaine.common.configloader.parsingconfigurator import ParsingConfigurator
import config

import sys
import time
import re
import itertools
import pprint
import socket
import logging
import hashlib

sys.path = sys.path+['/usr/lib/yandex/combaine/']
from parsers import PARSERS


logger = logging.getLogger("combaine")

def Main(host_name, config_name, group_name, previous_time, current_time):
    # DO INIT LOGGER
    uuid = hashlib.md5("%s%s%s%i%i" %(host_name, config_name, group_name, previous_time, current_time)).hexdigest()
    logger.info("%s Start: %s %s %s %i %i" %(uuid, host_name, config_name, group_name, previous_time, current_time))
    print "%s Start: %s %s %s %i %i" %(uuid, host_name, config_name, group_name, previous_time, current_time)
    cloud_config = config.loadCloudConfig()
    conf = ParsingConfigurator(config_name)
    parser = PARSERS[conf.parser]
    if parser is None:
        #print "No PARSER"
        logger.error('%s No properly parser available' % uuid)
        return "Failed"
    db = DataGridFactory(**conf.db)  # Get DataGrid
    if db is None:
        #print "DB init Error"
        logger.error('%s Failed to init local databse' % uuid)
        return 'failed'
    df = FetcherFactory(**conf.df)    # Get DataFetcher
    if df is None:
        print "DF init Error"
        logger.error('%s Failed to init datafetcher' % uuid)
        return 'failed'
    ds = DistributedStorageFactory(**conf.ds) # Get Distributed storage  
    if ds is None:
        print "DS init Error"
        logger.error('%s Failed to init distributed storage like MongoRS' % uuid)
        return
    if not ds.connect('combaine_mid/%s' % conf.parser.replace(".", "_").replace("-","_")): # CHECK NAME OF COLLECTION!!!!
        print 'FAIL'
        logger.error('%s Cannot connect to distributed storage like MongoRS' % uuid)
        return 'failed'
    aggs = [AggregatorFactory(**agg_config) for agg_config in conf.aggregators]
    data = df.getData(host_name, (previous_time, current_time))
    print data
    if data:
        handle_data = itertools.takewhile(df.filter, (parser(i) for i in data))
        tablename = ''.join(group_name[:30]) + hashlib.md5('%s_%s_%s' % (config_name, group_name, host_name)).hexdigest()
        if not db.putData(handle_data , tablename):
            print 'No data to put in the localdb'
            logger.warning('%s Empty data for localdb' % uuid)
            return 'failed'
    else:
        logger.warning('%s Empty data from datafetcher' % uuid)
        return 'failed'

    res = itertools.chain( *[_agg.aggregate(db, (previous_time, current_time)) for _agg in aggs])
    RES = dict(((i, dict()) for i in xrange(previous_time, current_time)))
    for i in res:
        RES[i['time']][i['name']]=i['data'] 
    l = ( { 'host' : host_name.replace('.','_').replace('-','_'), 'time': k, 'data' : v } for k,v in RES.iteritems())
    print map(ds.insert, l)
    ds.close()
    logger.info('%s Success' % uuid)
    print "Success"
    return 'success'

def parsing(io):
    """Cloud wrapper """
    message = ""
    try:
        message = io.read()
        host, config, group, prev_time, cur_time = message.split(';')
        prev_time = int(prev_time)
        cur_time = int(cur_time)
    except Exception as err:
        io.write("failed;Wrong message format:%s;%s;%s" % (message, socket.gethostname(), str(err)))
        return
    else:
        try:
            res = Main(host, config, group, prev_time, cur_time)
        except Exception as err:
            res = 'failedi;Error: %s' % err
        finally:
            #log.info(';'.join(res, message))
            io.write(';'.join((res, message, socket.gethostname())))

