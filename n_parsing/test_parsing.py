#! /usr/bin/env python

from DataGrid.datagrid import DataGridFactory
from GetDataAPI.datafetcher import GetterFactory
from Aggregators.Aggregators import AverageAggregator
from DistributedStorage import DistributedStorageFactory
import config

import sys
import time
import re
import itertools
import pprint

#sys.path = sys.path+['/usr/lib/yandex/combaine/']
from _parsers import PARSERS

dbconfig = {'type' : "MySQL"}
dfconfig = { "timetail_port": 3132,
           "timetail_url": "/timetail?log=",
           "logname" : "nginx/access.log",
            "type" : 'timetail'
           }

aggconfig = {
        'name' : '20x',
        'host' : "SELECT COUNT(*) FROM %TABLENAME% WHERE %TABLENAME%.http_status >= 200 AND %TABLENAME%.http_status < 400 AND TIME = %%",
        'group': "summa"
}
aggconfig2 = {
        'name' : '30x',
        'host' : "SELECT COUNT(*) FROM %TABLENAME% WHERE %TABLENAME%.http_status >= 300 AND %TABLENAME%.http_status < 400 AND TIME = %%",
        'group': "summa"
}

dsconfig = { "hosts" : ["cocaine-mongo01g.kit.yandex.net:27017", "cocaine-mongo02g.kit.yandex.net:27017", "cocaine-mongo03f.kit.yandex.net:27017"],
             "type"  : "MongoReplicaSet"
}

def Main(host_name, config_name, group_name, previous_time, current_time):
    # DO INIT LOGGER
    cloud_config = config.loadCloudConfig()
    config.initLogger(**cloud_config)
    parsing_config = config.loadParsingConfig(config_name)
    #dbconfig['local_db_name'] = 'FFFFFFFf'
    db = DataGridFactory(**dbconfig)  # Get DataGrid
    df = GetterFactory(**dfconfig)    # Get DataFetcher
    ds = DistributedStorageFactory(**dsconfig) # Get Distributed storage  
    print ds
    if not ds.connect('combaine_mid/test_%s' % config_name): # CHANGE NAME OF COLLECTION!!!!
        print 'FAIL'
        return 0

    parser = PARSERS[parsing_config['parser']]
    data = df.getData(host_name, (previous_time, current_time))
    if data:
        handle_data = itertools.takewhile(df.filter, (parser(i) for i in data))
        tablename = 'combaine_%s_%s_%s' % (config_name, group_name, host_name)
        db.putData(handle_data , tablename)
    else:
        print 'NO DATA'
        return 0

    agg = AverageAggregator(**aggconfig)
    agg2 = AverageAggregator(**aggconfig2)

    res = agg.aggregate(db, (previous_time, current_time))
    res2 = agg2.aggregate(db, (previous_time, current_time))

    RES = dict()
    for i in xrange(previous_time, current_time):
        RES[i] = dict()
    for i in res: 
        RES[i['time']][i['name']]=i['data'] 
    for i in res2:
        RES[i['time']][i['name']]=i['data'] 
    l = ( { 'host' : host_name, 'time': k, 'data' : v } for k,v in RES.items())
    map(ds.insert, l)

if __name__=="__main__":
    Main('links04d.feeds.yandex.net', 'feeds_nginx', 'feeds-links', int(time.time())-30, int(time.time())-10)
