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
import weakref
#import gc

TYPES = ( "RAW", "PROCESSED" )

sys.path = sys.path+['/usr/lib/yandex/combaine/']
from parsers import PARSERS


logger = logging.getLogger("combaine")

def Main(host_name, config_name, group_name, previous_time, current_time):
    # DO INIT LOGGER
    uuid = hashlib.md5("%s%s%s%i%i" %(host_name, config_name, group_name, previous_time, current_time)).hexdigest()
    logger.info("%s Start: %s %s %s %i %i" %(uuid, host_name, config_name, group_name, previous_time, current_time))
    print "%s Start: %s %s %s %i %i" %(uuid, host_name, config_name, group_name, previous_time, current_time)
    conf = ParsingConfigurator(config_name)

    # Construct parser function
    parser = PARSERS.get(conf.parser, None)

    if parser is None:
        print "No PARSER"
        logger.error('%s No properly parser available' % uuid)
        return "Failed"

    # Construct Distributed Storage
    ds = DistributedStorageFactory(**conf.ds) # Get Distributed storage  
    if ds is None:
        print "DS init Error"
        logger.error('%s Failed to init distributed storage like MongoRS' % uuid)
        return 'failed; DS init Error'
    if not ds.connect('combaine_mid/%s' % config_name): # CHECK NAME OF COLLECTION!!!!
        print 'FAIL'
        logger.error('%s Cannot connect to distributed storage like MongoRS' % uuid)
        return 'failed; Connect to DS'
    
    # Construct Data Fetcher
    df = FetcherFactory(**conf.df)    # Get DataFetcher
    if df is None:
        print "DF init Error"
        logger.error('%s Failed to init datafetcher' % uuid)
        return 'failed; Failed to init DF'

    # Construct aggregators
    aggs = [AggregatorFactory(**agg_config) for agg_config in conf.aggregators]

    #fetch data
    data = df.getData(host_name, (previous_time, current_time))
    print data

    handle_data = itertools.takewhile(df.filter, (parser(i) for i in data))
    handle_data = [l for l in handle_data if l is not None]
    
    if not data:
        logger.warning('%s Empty data from datafetcher' % uuid)
        return 'failed; Empty data from DF'

    # TBD wrap in separate fucntion ->
    if any(_agg.agg_type == TYPES.index("RAW") for _agg in aggs):
        db = DataGridFactory(**conf.db)  # Get DataGrid
        if db is None:
            #print "DB init Error"
            logger.error('%s Failed to init local databse' % uuid)
            return 'failed; Failed to init DG'

        [_agg.set_datagrid_backend(db) for _agg in aggs if _agg.agg_type == TYPES.index("RAW")]


        tablename = ''.join(group_name[:30]) + hashlib.md5('%s_%s_%s' % (config_name, group_name, host_name)).hexdigest()
        if not db.putData(handle_data , tablename):
            print 'No data to put in the localdb'
            logger.warning('%s Empty data for localdb' % uuid)
            return 'failed; No data for local db'
    # TBD end of wrap

    if any(_agg.agg_type == TYPES.index("PROCESSED") for _agg in aggs):
        [_agg.set_data(hadle_data) for _agg in aggs if _agg.agg_type == TYPES.index("PROCESSED")]

    res = itertools.chain( [_agg.aggregate((previous_time, current_time)) for _agg in aggs])
    print  [ds.insert("%(host)s;%(conf)s;%(time)s;%(etime)s;%(aggname)s" % {\
                                                                    'host'  : host_name.replace('.','_').replace('-','_'),\
                                                                    'conf'  : config_name,\
                                                                    'time'  : previous_time,\
                                                                    'etime' : current_time,\
                                                                  'aggname' : l[0]},
                                                                                     l[1]) for l in res]
    ds.close()
    logger.info('%s Success' % uuid)
    print "Success"
    #gc.collect()
    return 'success'


# Cocaine cloud IO wrapper
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
            res = 'failed;Error: %s' % err
        finally:
            log.info(';'.join(res, message))
            io.write(';'.join((res, message, socket.gethostname())))

