#! /usr/bin/env python

import GetDataAPI.datafetcher as t

config = { "timetail_port": 3132,
           "timetail_url": "/timetail?log=",
           "logname" : "nginx/access.log",
            "type" : 'timetail'
           }


d = t.GetterFactory(**config)
for i in d.getData("front01e.photo.yandex.net", 30):
    print i
