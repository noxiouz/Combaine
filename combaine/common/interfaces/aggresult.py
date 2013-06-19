from itertools import izip_longest

class AggRes(object):

    def __init__(self, aggname, subgroupnames, groupname, aggconfig):
        self._aggname = aggname.split("@")[-1]
        self._aggconfig = aggconfig
        self._groupname = groupname
        self._subgroupnames = subgroupnames
        self._res = dict()
        self._time = None

    def store_result(self, result):
        self._time, values = result.popitem()
        l = izip_longest(self._subgroupnames, values, fillvalue=self._groupname)
        self._values = dict((x for x in l))

    @property
    def aggname(self):
        return self._aggname

    @property
    def subgroupnames(self):
        return self._subgroupnames

    @property
    def groupname(self):
        return self._groupname

    @property
    def aggconfig(self):
        return self._aggconfig

    @property
    def values(self):
        return self._values.iteritems()

    @property
    def time(self):
        return self._time


class HandlerRes(object):

    def __init__(self, aggname, subgroupnames, groupname, aggconfig):
        self._aggname = aggname
        self._aggconfig = aggconfig
        self._groupname = groupname
        self._subgroupnames = subgroupnames
        self._time = None


    def store_result(self, result, time):
        self._values = result
        self._time = time

    @property
    def aggname(self):
        return self._aggname

    @property
    def subgroupnames(self):
        return self._subgroupnames

    @property
    def groupname(self):
        return self._groupname

    @property
    def aggconfig(self):
        return self._aggconfig

    @property
    def values(self):
        return self._values.iteritems()

    @property
    def time(self):
        return self._time
