#!/usr/bin/env python

import sys
import time
import re
import itertools
import pprint
import socket
import logging
import hashlib
import traceback

from combaine.plugins.DataGrid import DataGridFactory
from combaine.plugins.DataFetcher import FetcherFactory
from combaine.plugins.Aggregators import AggregatorFactory
from combaine.plugins.DistributedStorage import DistributedStorageFactory
from combaine.common.configloader.parsingconfigurator import ParsingConfigurator
from combaine.common.loggers import ParsingLogger
import combaine.common.parsers_loader as ALL_PARSERS


TYPES = ("RAW", "PROCESSED")

def Main(host_name, config_name, group_name, previous_time, current_time):
    reload(ALL_PARSERS) # for d0uble - he wants to reload parsing functions
    uuid = hashlib.md5("%s%s%s%i%i" %(host_name, config_name, group_name, previous_time, current_time)).hexdigest()[:10]
    logger = ParsingLogger(uuid)
    logger.info("Start parsing: %s %s %s %i %i" %(host_name, config_name, group_name, previous_time, current_time))
    conf = ParsingConfigurator(config_name)

    # Construct parser function
    parser = ALL_PARSERS.PARSERS.get(conf.parser, None)

    if parser is None:
        logger.error('No properly parser available with name %s' % conf.parser)
        return "failed; No parser"

    # Construct Distributed Storage
    ds = DistributedStorageFactory(**conf.ds) # Get Distributed storage  
    if ds is None:
        logger.error('Failed to init distributed storage like MongoRS')
        return 'failed; DS init Error'
    if not ds.connect('combaine_mid/%s' % config_name): # CHECK NAME OF COLLECTION!!!!
        logger.error('Cannot connect to distributed storage like MongoRS')
        return 'failed; Connect to DS'
    
    # Construct Data Fetcher
    df = FetcherFactory(**conf.df)    # Get DataFetcher
    if df is None:
        logger.error('%s Failed to init datafetcher' % uuid)
        return 'failed; Failed to init DF'

    # Construct aggregators
    aggs = filter(lambda x: x is not None, (AggregatorFactory(**agg_config) for agg_config in conf.aggregators))

    #fetch data
    data = df.getData(host_name, (previous_time, current_time))

    if not data:
        logger.warning('%s Empty data from datafetcher' % uuid)
        return 'failed; Empty data from DF'

    handle_data = (l for l in parser(data) if df.filter(l))
    handle_data = [l for l in handle_data if l is not None]

    if len(handle_data) == 0:
        logger.info("Zero size of handling data list after parsing and filter")
        return 'failed; Zero size of handling data list after parsing and filter'

    # TBD wrap in separate fucntion ->
    if any(_agg.agg_type == TYPES.index("RAW") for _agg in aggs):
        db = DataGridFactory(**conf.db)  # Get DataGrid
        if db is None:
            logger.error('Failed to init local databse')
            return 'failed; Failed to init DG'

        [_agg.set_datagrid_backend(db) for _agg in aggs if _agg.agg_type == TYPES.index("RAW")]


        tablename = ''.join(group_name[:30]) + hashlib.md5('%s_%s_%s' % (config_name, group_name, host_name)).hexdigest()
        if not db.putData(handle_data , tablename):
            logger.warning('Empty data for localdb')
            return 'failed; No data for local db'

    # TBD end of wrap
    if any(_agg.agg_type == TYPES.index("PROCESSED") for _agg in aggs):
        [_agg.set_data(handle_data) for _agg in aggs if _agg.agg_type == TYPES.index("PROCESSED")]

    res = itertools.chain([_agg.aggregate((previous_time, current_time)) for _agg in aggs])
    logger.debug("Send data to storage: %s" % [ds.insert("%(host)s;%(conf)s;%(time)s;%(etime)s;%(aggname)s" % {\
                                                                    'host'  : host_name.replace('.','_').replace('-','_'),\
                                                                    'conf'  : config_name,\
                                                                    'time'  : previous_time,\
                                                                    'etime' : current_time,\
                                                                  'aggname' : l[0]},
                                                                                     l[1]) for l in res])
    ds.close()
    logger.info('Parsing has finished successfully')
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
            res = 'failed;Error: %s' % traceback.format_exc()
        finally:
            io.write(';'.join((res, message, socket.gethostname())))

