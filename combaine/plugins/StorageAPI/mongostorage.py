from __abstractstorage import BaseStorage

import logging

import pymongo

class MongoStorage(BaseStorage):

    def __init__(self, **config):
        """
        hosts - ['host:port','host:port']
        """
        self.log = logging.getLogger('combaine')
        self.mongo_client = None
        def __connect(hosts):
            connected = False
            try:
                self.mongo_client = pymongo.Connection(hosts)
            except pymongo.errors.ConnectionFailure, err:
                print err
                return False
            else:
                return True

        try:
            mongo_hosts = ','.join(config["host"])
            db_name, collection_name = config["app_id"].split("@")
            print db_name, collection_name
        except KeyError, err:
            print "KeyError %s" % err
        else:
            __connect(mongo_hosts)

        if not self.mongo_client:
            raise Exception
        self.root = self.mongo_client[db_name][collection_name]

    def put(self, key, value="Empty"):
        doc = { "_id"   : key,
                "value" : value,
        }
        try:
            self.root.insert(doc)
        except Exception, err:
            self.log.error("Error, while putting")
            print err
            return False
        else:
            return True

    def delete(self, key):
        try:
            doc_id = self.root.find_one({"_id" : key})["_id"]
            self.root.remove(doc_id)
        except Exception, err:
            self.log.error("cannot remove")
            return False
        else:
            return True


    def list(self):
        try:
            _result = [ doc["_id"] for doc in self.root.find()]
        except Exception, err:
            self.log.error("ALARM!!!")
            return []
        else:
            return _result

    def get(self, key):
        try:
            _res = self.root.find_one({"_id" : key})["value"]
        except Exception, err:
            self.log.error('Cannot get value from: %s %s ' % (key, err))
            return None
        else:
            return _res

    def modify(self, key, value):
        try:
            self.root.update({"_id" : key},{"value" : value})
        except Exception, err:
            print err
            self.log.error("Erro on modify %s: %s" % (key, err))
            return False
        else:
            return True

    def destroy(self):
        if self.mongo_client:
            self.mongo_client.disconnect()

PLUGIN_CLASS = MongoStorage
