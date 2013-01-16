from __AbstractStorage import AbstractDistributedStorage

import pymongo

class MongoReplicaSet(AbstractDistributedStorage):

    def __init__(self, **config):
        self.hosts = config['hosts']
        self.rs = None
        self.db = None

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

    def insert(self, data):
        try:
            print data
            self.db_cursor.insert(data, continue_on_error=True)
        except Exception, err:
            return False
        else:
            return True

PLUGIN_CLASS = MongoReplicaSet
