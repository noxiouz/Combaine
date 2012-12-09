#! /usr/bin/env python

from DataGrid.datagrid import DataGridFactory
from GetDataAPI.datafetcher import GetterFactory
import config

import sys
import time

#sys.path = sys.path+['/usr/lib/yandex/combaine/']
from _parsers import PARSERS

dbconfig = {'type' : "MySQL"}
dfconfig = { "timetail_port": 3132,
           "timetail_url": "/timetail?log=",
           "logname" : "nginx/access.log",
            "type" : 'timetail'
           }

def Main(host_name, config_name, group_name, previous_time, current_time):
    # DO INIT LOGGER
    cloud_config = config.loadCloudConfig()
    config.initLogger(**cloud_config)
    print time.time()
    parsing_config = config.loadParsingConfig(config_name)
    db = DataGridFactory(**dbconfig)
    df = GetterFactory(**dfconfig)
    print time.time()
    parser = PARSERS['nginx_access_feeds_parser']
    data = df.getData(host_name, 300)
    print time.time()
    if data:
        db.putData((parser(i) for i in data) ,'AAAAAa')
    print time.time()
    q = 'SELECT COUNT(*) FROM AAAAAa'
    print db.perfomCustomQuery(q)
    print time.time()


if __name__=="__main__":
    Main('links04d.feeds.yandex.net', 'feeds_nginx', 'feeds-links', 1, 1)
