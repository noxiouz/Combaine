#!/usr/bin/python

# Copyright (c) 2014 Yandex LLC. All rights reserved.

# This file is part of Combaine.

# Combaine is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.

# Combaine is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.

# You should have received a copy of the GNU Lesser General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import json

from handystats.chrono import Timepoint
from handystats.statistics import Data

class Handystats(object):
    def __init__(self, config):
        self.config = config

        if 'metric' not in self.config or 'stat' not in self.config:
            raise RuntimeError("'metric' and 'stat' config options are mandatory")

        # Support for multiple metrics
        self.metrics = [self.config['metric']] if isinstance(self.config['metric'], str) else list(self.config['metric'])
        if not all(map(lambda x: isinstance(x, str), self.metrics)):
            raise RuntimeError("'metric' config option must be string or list of strings: {0}".format(self.metrics))

        self.stat = self.config['stat']

    def aggregate_host(self, payload, prevtime, currtime):
        dump_timestamp, dump_str = payload.split(' ', 1)

        dump_timestamp = int(dump_timestamp) / 1000
        dump = json.loads(dump_str)

        query_interval = currtime - prevtime
        if query_interval <= 0:
            raise RuntimeError("task's time frame interval must be positive: prevtime = {0}, currtime = {1}".format(prevtime, currtime))

        metrics_data = None

        for metric in self.metrics:
            if metric not in dump:
                continue

            data = Data.from_json(json.dumps(dump[metric]))
            data.truncate(
                before = Timepoint.from_unixtime(dump_timestamp - query_interval),
                after = Timepoint.from_unixtime(dump_timestamp)
                )
            if not metrics_data:
                metrics_data = data
            else:
                metrics_data.merge(data)

        return metrics_data.to_json() if metrics_data else None

    def aggregate_group(self, payload):
        merged_data = None

        for host_payload in payload:
            if host_payload is None:
                continue

            data = Data.from_json(host_payload)
            if merged_data is None:
                merged_data = data
            else:
                merged_data.merge(data)

        if not merged_data:
            # NOTE: return 0 instead?
            raise RuntimeError("No data for metrics {0}".format(self.metrics))

        if self.stat == 'value':
            return merged_data.value()

        elif self.stat == 'min':
            return merged_data.min()

        elif self.stat == 'max':
            return merged_data.max()

        elif self.stat == 'sum':
            return merged_data.sum()

        elif self.stat == 'count':
            return merged_data.count()

        elif self.stat == 'avg':
            return merged_data.avg()

        elif self.stat == 'moving-count':
            return merged_data.moving_count()

        elif self.stat == 'moving-sum':
            return merged_data.moving_sum()

        elif self.stat == 'moving-avg':
            return merged_data.moving_avg()

        elif self.stat == 'quantile':
            levels = map(int, list(self.config.get('levels', [75, 90, 93, 94, 95, 96, 97, 98, 99])))

            if not all(map(lambda x: x > 0 and x < 100, levels)):
                raise RuntimeError("'levels' config option must be list of integers between 0 and 100 (exclusive)")

            res = []
            for level in levels:
                res.append(merged_data.quantile(level / 100.0))

            return res

        elif self.stat == 'entropy':
            return merged_data.entropy()

        elif self.stat == 'throughput':
            return merged_data.throughput()

        elif self.stat == 'frequency':
            return merged_data.frequency()

        else:
            raise NotImplementedError("'{0}' statistic is not implemented".format(self.stat))
