#!/usr/bin/env python

import re

DEFAULT_QUANTILE_VALUES = [75, 90, 93, 94, 95, 96, 97, 98, 99]


class Multimetrics(object):
    """
    Special aggregator of prehandled quantile data
    Data looks like:
    uploader_timings_request_post_patch-url 0.001 0.001 0.002
    uploader_timings_request_post_upload-from-service
    uploader_timings_request_post_upload-url 0.001 0.002 0.001 0.002 0.001
    uploader_timings_request_put_patch-target 0.651 0.562 1.171
    """
    def __init__(self, config):
        self.quantile = list(config.get("values", [])) or DEFAULT_QUANTILE_VALUES
        self.quantile.sort()
        # recalculate to rps? default yes
        self.rps = ("yes" == config.get("rps", "yes"))
        # find timings by specified string. default '_timings'
        self.timings_is = config.get("timings_is", "_timings")
        self.clean_re = re.compile(config.get("clean_timings_re", ':|,| - '))
        # multiply on factor: default `1`
        factor = config.get("factor", 1)
        if factor == 1:
            self.factor = float
        else:
            self.factor = lambda item: factor * float(item)

    def is_timings(self, name):
        return self.timings_is in name

    def clean_timings(self, timings_as_string):
        return self.clean_re.sub(' ', timings_as_string)


    def _parse_metrics(self, lines):
        speedup = self.factor
        result = {}
        for line in lines:
            line = line.strip()
            if not line: continue

            name, _, metrics_as_strings = line.partition(" ")
            metrics_as_strings = self.clean_timings(metrics_as_strings)
            try:
                if self.is_timings(name):
                    metrics_as_values = map(speedup, metrics_as_strings.split())
                else:
                    metrics_as_values = sum(map(float, metrics_as_strings.split()))

                if name in result:
                    result[name] += metrics_as_values
                else:
                    result[name] = metrics_as_values
            except (ValueError, TypeError) as err:
                raise Exception("Unable to parse %s: %s" % (line, err))
        return result

    def aggregate_host(self, payload, prevtime, currtime):
        """ Convert strings of payload into dict[string][]float and return """
        result = self._parse_metrics(payload.splitlines())
        if self.rps:
            delta = float(currtime - prevtime)
            if delta <= 0:
                delta = 1
            for name in (key for key in result.keys() if not self.is_timings(key)):
                result[name] /= delta
        return result

    def aggregate_group(self, payload):
        """ Payload is list of dict[string][]float"""
        if len(payload) == 0:
            raise Exception("No data to aggregate")
        names_of_metrics = set()
        map(names_of_metrics.update, (i.keys() for i in payload))
        result = {}
        for metric in names_of_metrics:
            if self.is_timings(metric):
                all_resuts = list()
                for item in payload:
                    all_resuts.extend(item.get(metric, []))

                if len(all_resuts) == 0:
                    continue
                result[metric] = list()

                all_resuts.sort()
                count = float(len(all_resuts))
                for q in self.quantile:
                    if q < 100:
                        index = int(count / 100 * q)
                    else:
                        index = count - 1
                    result[metric].append(all_resuts[index])
            else:
                metric_sum = sum(item.get(metric, 0) for item in payload)
                result[metric] = metric_sum

        return result


class Multimetrics_rps(Multimetrics):
    pass

class Multimetrics_rps_fixed(Multimetrics):
    pass


if __name__ == '__main__':
    import sys
    import pprint
    def print_res(res):
        for k, v in res.items():
            if "global_" in k:
                print(k, v)
    m = Multimetrics({"factor": 1000})
    print m.__dict__
    with open(sys.argv[1], 'r') as f:
        payload = f.read()
    r = m.aggregate_host(payload, 1, 3)
    payload = [r]

    print("+++ Aggregate group +++")
    print_res(m.aggregate_group(payload))
