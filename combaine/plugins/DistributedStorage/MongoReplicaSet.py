from __AbstractStorage import AbstractDistributedStorage

import pymongo
import hashlib
import time

class MongoReplicaSet(AbstractDistributedStorage):

    def __init__(self, **config):
        self.hosts = config['hosts']
        self.rs = None
        self.db = None
        self.coll_name = None
        self.cache_key_list = list()

    def connect(self, namespace):
        try:
            self.rs = pymongo.Connection(self.hosts, fsync=True)
            db, collection = namespace.split('/')
            self.coll_name = collection
            self.db = self.rs[db]
            if collection in self.db.collection_names():
                if not self.db[collection].options().get("capped"):
                    self.db.drop_collection(collection)
                    self.db.create_collection(collection, capped=True, size=500000000, max=2000)
            else:
                self.db.create_collection(collection, capped=True, size=500000000, max=2000)
            self.db_cursor = self.db[collection]
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
            print key
            print _id
            value = {"_id" : _id, "key" : key, "value" : data, "time" : int(time.time()) }
            #print self.db_cursor.insert(value, continue_on_error=True, w=0, manipulate=False)
            print self.db_cursor.save(value, continue_on_error=True, w=1, manipulate=False)
        except Exception, err:
            return False
        else:
            return True

    def read(self, key, cache=False):
        try:
            _id = hashlib.md5(key).hexdigest()
            ret = self.db_cursor.find_one({"_id" : _id }, fields={"key" : False, "_id" : False, "time" : False})
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
            return "OK" #for capped
            _id = hashlib.md5(key).hexdigest()
            return str(self.db_cursor.remove(_id, w=1))
        except Exception as err:
            print str(err)
            return False
        else:
            return True

    def clear_namespace(self):
        try:
            print self.db.drop_collection(self.coll_name)
            return True
        except Exception as err:
            print str(err)
            return False

PLUGIN_CLASS = MongoReplicaSet
