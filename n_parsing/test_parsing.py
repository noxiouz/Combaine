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

sys.path = sys.path+['/usr/lib/yandex/combaine/']
from parsers import PARSERS

#dbconfig = {'type' : "mysqldg"}
#dfconfig = { "timetail_port": 3132,
#           "timetail_url": "/timetail?log=",
#           "logname" : "nginx/access.log",
#            "type" : 'timetail'
#           }

aggconfig = {
        'type' : "AverageAggregator",
        'name' : '20x',
        'host' : "SELECT COUNT(*) FROM %TABLENAME% WHERE %TABLENAME%.http_status >= 200 AND %TABLENAME%.http_status < 400 AND TIME = %%",
        'group': "summa"
}

#dsconfig = { "hosts" : ["cocaine-mongo01g.kit.yandex.net:27017", "cocaine-mongo02g.kit.yandex.net:27017", "cocaine-mongo03f.kit.yandex.net:27017"],
#             "type"  : "MongoReplicaSet"
#}

def Main(host_name, config_name, group_name, previous_time, current_time):
    # DO INIT LOGGER
    cloud_config = config.loadCloudConfig()
    config.initLogger(**cloud_config)
    parsing_config = config.loadParsingConfig(config_name)
    conf = ParsingConfigurator(config_name)
    db = DataGridFactory(**conf.db)#**dbconfig)  # Get DataGrid
    if db is None:
        print "DB init Error"
        return 'failed'
    df = FetcherFactory(**conf.df)#**dfconfig)    # Get DataFetcher
    if df is None:
        print "DF init Error"
        return 'failed'
    ds = DistributedStorageFactory(**conf.ds)#**dsconfig) # Get Distributed storage  
    if ds is None:
        print "DS init Error"
        return
    print ds
    if not ds.connect('combaine_mid/test_%s' % config_name): # CHANGE NAME OF COLLECTION!!!!
        print 'FAIL'
        return 'failed'

    aggs = [AggregatorFactory(**agg_config) for agg_config in conf.aggregators]
    parser = PARSERS[parsing_config['parser']]
    data = df.getData(host_name, (previous_time, current_time))
    if data:
        handle_data = itertools.takewhile(df.filter, (parser(i) for i in data))
        tablename = 'combaine_%s_%s_%s' % (config_name, group_name, host_name)
        db.putData(handle_data , tablename)
    else:
        print 'NO DATA'
        return 'failed'

    res = itertools.chain( *[_agg.aggregate(db, (previous_time, current_time)) for _agg in aggs])
    RES = dict(((i, dict()) for i in xrange(previous_time, current_time)))
    for i in res: 
        RES[i['time']][i['name']]=i['data'] 
    l = ( { 'host' : host_name, 'time': k, 'data' : v } for k,v in RES.iteritems())
    map(ds.insert, l)
    return 'success'

def parsing(io):
    """Cloud wrapper """
    message = ""
    try:
        message = io.read()
        host, config, group, prev_time, cur_time = message.split(';')
        prev_time = int(prev_time)
        cut_time = int(cur_time)
    except Exception as err:
        io.write("failed;Wrong message format:%s;%s;%s" % (message, socket.gethostname(), str(err)))
        return
    else:
        res = Main(host, config, group, prev_time, cur_time)
        io.write(';'.join((res, message)))


if __name__=="__main__":
    Main('links04d.feeds.yandex.net', 'feeds_nginx', 'feeds-links', int(time.time())-30, int(time.time())-10)
