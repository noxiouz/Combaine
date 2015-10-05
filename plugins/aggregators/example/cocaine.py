#!/usr/bin/env python

try:
    # 3 times faster
    import simplejson as json
except ImportError:
    import json

from collections import defaultdict
import sys

import msgpack


TIMINGS = ('50.00%', '75.00%', '90.00%', '95.00%', '98.00%', '99.00%', '99.95%')


def default_info_factory():
    # default dict could be used here too
    # but it's here as scheme
    return {
        "load": 0,
        "avg_oldest_channel_age": 0.,
        "max_oldest_channel_age": 0,
        "avg_uptime": 0,
        "min_uptime": sys.maxint,
        "pool": {
            "active": 0,
            "capacity": 0,
            "idle": 0,
            "total:crashed": 0,
            "total:spawned": 0,
            },
        "queue": {
            "capacity": 0,
            "depth": 0,
            "avg_oldest_event_age": 0.,
            "max_oldest_event_age": 0,
            },
        "requests": {
            "accepted": 0,
            "rejected": 0,
            },
        # "50.00%", '75.00%', '90.00%', '95.00%', '98.00%', '99.00%', '99.95%',
        "timings": [0, 0, 0, 0, 0, 0, 0],
        }


def avg(curr_avg, count, value):
    # avg = (curr_avg * curr_count + curr_value)/(curr_count + 1)
    return (curr_avg * count + value)/(count + 1)


class CocaineToolInfo(object):
    def __init__(self, config):
        self.config = config

    def aggregate_host(self, payload, prevtime, currtime):
        data = json.loads(payload)["apps"]
        # remove unused fields
        apps = data.keys()
        for app in apps:
            info = data.pop(app)
            if info["state"] != "running":
                continue
            del info["profile"]
            del info["state"]
            if info["pool"]["slaves"]:
                slowest_slave = max((i for i in info["pool"]["slaves"].values() if i["state"] == "active"),
                                    key=lambda x: x["oldest_channel_age"])
                info["oldest_channel_age"] = slowest_slave["oldest_channel_age"]
            else:
                info["oldest_channel_age"] = 0
            del info["pool"]["slaves"]
            data[app] = info

        return msgpack.packb(data)

    def aggregate_group(self, payload):
        infos = [msgpack.unpackb(i) for i in payload]
        apps = set()
        [apps.update(i.keys()) for i in infos]
        res = defaultdict(default_info_factory)
        # we assume that each host has all applications
        # to calculate an average
        for curr_count, info in enumerate(infos):
            for app_name, app in info.iteritems():

                app_info = res[app_name]
                app_info["load"] += app["load"]
                # maximum and avg channel_age
                oldest_channel_age = app["oldest_channel_age"]
                app_info["max_oldest_channel_age"] = max(app_info["max_oldest_channel_age"],
                                                         oldest_channel_age)
                app_info["avg_oldest_channel_age"] = avg(app_info["avg_oldest_channel_age"],
                                                         curr_count, oldest_channel_age)
                # minimal an avg uptime
                uptime = app["uptime"]
                app_info["min_uptime"] = min(app_info["min_uptime"], uptime)
                app_info["avg_uptime"] = avg(app_info["avg_uptime"], curr_count, uptime)

                # sum pool info
                app_pool = app["pool"]
                app_info_pool = app_info["pool"]
                for k in app_info_pool:
                    app_info_pool[k] += app_pool[k]

                # sum queue info
                app_queue = app["queue"]
                app_info_queue = app_info["queue"]
                for k in ("capacity", "depth"):
                    app_info_queue[k] += app_queue[k]

                # maximum and avg event_age
                oldest_event_age = app_queue["oldest_event_age"]
                app_info_queue["max_oldest_event_age"] = max(app_info_queue["max_oldest_event_age"],
                                                             oldest_event_age)
                app_info_queue["avg_oldest_event_age"] = avg(app_info_queue["avg_oldest_event_age"],
                                                             curr_count, oldest_event_age)

                # sum requests info
                app_requests = app["requests"]
                app_info_requests = app_info["requests"]
                for k in app_info_requests:
                    app_info_requests[k] += app_requests[k]

                # avg timings
                app_timings = app["timings"]
                app_info_timings = app_info["timings"]
                for i, k in enumerate(TIMINGS):
                    app_info_timings[i] = avg(app_info_timings[i], curr_count, app_timings[k])
        return res


if __name__ == '__main__':
    import pprint
    aggregate = CocaineToolInfo(None)
    with open('info.log', 'rb') as f:
        data = f.read()
    one_host = aggregate.aggregate_host(data, 100, 101)
    all_host = aggregate.aggregate_group([one_host]*2)
    pprint.pprint(dict(all_host))
