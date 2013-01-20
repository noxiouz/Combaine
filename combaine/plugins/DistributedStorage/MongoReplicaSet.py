from __AbstractStorage import AbstractDistributedStorage

import pymongo
import hashlib

class MongoReplicaSet(AbstractDistributedStorage):

    def __init__(self, **config):
        self.hosts = config['hosts']
        self.rs = None
        self.db = None
        self.cache_key_list = list()

    def connect(self, namespace):
        try:
            self.rs = pymongo.Connection(self.hosts)
            db, collection = namespace.split('/')
            self.db_cursor = self.rs[db][collection]
        except Exception, err:
            print str(err)
            return False
        else:
            return True

    def close(self):
        try:
            self.rs.close()
        except Exception, err:
            print err
            return False
        else:
            return True

    def insert(self, key, data):
        try:
            _id = hashlib.md5(key).hexdigest()
            value = {"_id" : _id, "key" : key, "value" : data }
            print self.db_cursor.insert(value, continue_on_error=True)
        except Exception, err:
            return False
        else:
            return True

    def read(self, key, cache=False):
        try:
            _id = hashlib.md5(key).hexdigest()
            ret = self.db_cursor.find_one({"_id" : _id }, fields={"key" : False, "_id" : False})
            if ret is not None:
                if cache:
                    self.cache_key_list.append(key)
                return ret["value"]
            else:
                return []
        except Exception as err:
            print str(err)
            return []

    def remove(self, key):
        try:
            _id = hashlib.md5(key).hexdigest()
            print self.db_cursor.remove(_id, w=0)
        except Exception as err:
            print str(err)
            return False
        else:
            return True

PLUGIN_CLASS = MongoReplicaSet
