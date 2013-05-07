import uuid

class Message(object):
    
    def __init__(self, data):
        """uuid;host1;pr_conf;group;1366693889;1366693909;__H__
        uuid;group;par_conf;agg_conf;1366693889;1366693909;__G__
        """
        self._data = dict()
        if data[-1] == "__H__":
            self._data["host"] = data[1]
            self._data["parsing"] = data[2]
            self._data["group"] = data[3]
            self._data["starttime"] = int(data[4])
            self._data["endtime"] = int(data[5])
        elif data[-1] == "__G__":
            self._data["group"] = data[1]
            self._data["parsing"] = data[2]
            self._data["aggregation"] = data[3]
            self._data["starttime"] = int(data[4])
            self._data["endtime"] = int(data[5])
        else:
            raise Exception("Wrong message format")
        print self._data

    def pack(self):
        self._data["uuid"] = uuid.uuid4().hex
        return self._data

    @staticmethod
    def unpack(data):
        """ Now as is. In the future - changed """
        return data
