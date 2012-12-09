# -*- coding: utf-8 -*-
#
# Copyright (c) 2012 Tyurin Anton noxiouz@yandex-team.ru
#
# This file is part of Combaine.
#
# Combaine is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# Combaine is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

import ZKeeperAPI.zkapi as ZK

import logging

class BaseStorage():

    def __init__(self, **config):
        raise Exception

    def put(self, abspath):
        raise Exception

    def get(self, abspath):
        raise Exception

    def delete(self, abspath):
        raise Exception

    def modify(self, abspath):
        raise Exception

    def list(self, abspath):
        raise Exception

    def destroy(self):
        raise Exception

#-------------------------------------------------------------
# TEST VERSION!!!
#-------------------------------------------------------------

class NullStorage():

    def __init__(self, **config):
        self.finish = None

    def put(self, key, value='Empty'):
        if key == 'FINISHMARK':
            self.finish = value
        return True

    def get(self, key):
        if key == 'FINISHMARK' and self.finish:
            return self.finish
        else:
            return None

    def modify(self, key, value):
        if key == 'FINISHMARK':
            self.finish = value
        return True

    def destroy(self):
        pass

    def list(self):
        return ['FINISHMARK'] if self.finish else []

    def delete(self, key):
        self.finish = None if key == 'FINISHMARK' else self.finish
        return True

#--------------------------------------------------------------

class ZKStorage(BaseStorage):

    def __init__(self, **config):
        try:
            self.log = logging.getLogger('combaine')
            self.zkclient = ZK.ZKeeperClient(**config)
            self.id = config['app_id']
            res = self.zkclient.write('/'+self.id,"Rootnode")
            if (res != ZK.ZK_NODE_EXISTS ) and (res < 0):
                self.log.error('Cannot init storage')
                raise Exception
        except Exception, err:
            self.log.error('Fail!!!')

    def put(self, key, value='Empty'):
        if self.zkclient.write('/'+self.id+'/'+key, value) == 0:
            self.log.debug('Success put')
            return True
        else:
            self.log.error('Fail put')
            return False

    def delete(self, key):
        if self.zkclient.delete('/'+self.id+'/'+key) == 0:
            self.log.debug('Succesfully delete '+str(key) )
            return True
        else:
            self.log.info('Fail to delete '+str(key) )
            return False

    def list(self):
        res = self.zkclient.list('/'+self.id)
        if res[1] == 0:
            self.log.debug('Ls succesfully')
            return res[0]
        else:
            self.log.error('Fail to list')
            return None

    def get(self, key):
        res = self.zkclient.read('/'+self.id+'/'+key)
        if res[1] == 0:
            self.log.debug('Get value from %s succesfully' % key)
            return res[0]
        else:
            self.log.error('Cannot get value from: '+key)
            return None

    def modify(self, key, value):
        return self.zkclient.modify('/'+self.id+'/'+key, value)

    def destroy(self):
        self.zkclient.disconnect()
        self.log.debug('Succesfully disconnect from storage')

#----------------------------------------------------
# Mongo based storage
#----------------------------------------------------

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

#------------------------------------------------------
#------------------------------------------------------
#------------------------------------------------------

def StorageFactory(**config):
        type = { "Zookeeper"    : ZKStorage,
                 "MongoDB"      : MongoStorage,
                 "Null"         : NullStorage,
                 "Default"      : ZKStorage,
        }
        return type[config["type"]](**config)
