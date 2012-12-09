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
from socket import gethostname

import logging

logger = logging.getLogger('combaine')


class losingLockExc(Exception):
    def __str__(self):
        return "Lock is not mine"

class BaseLockServer():
    """ """
    def __init__(self):
        raise Exception

    def getlock(self):
        raise Exception

    def setLockName(self):
        raise Exception

    def releaselock(self):
        raise Exception

    def destroy(self):
        raise Exception

    def log(self, level, message):
        logger.debug(message)

    def checkLock(self):
        raise Exception

    def destroy(self):
        raise Exception

class ZKLockServer(BaseLockServer):
    """Zookeeper based lockserver. """
    def __init__(self, **config):
        try:
            self.zkclient = ZK.ZKeeperClient(**config)
            self.id = config['app_id']
            res = self.zkclient.write('/'+self.id,"Rootnode")
            if (res != ZK.ZK_NODE_EXISTS ) and (res < 0):
                self.log( 'WARN!!!','Cannot init ZK lock server')
                raise Exception
            self.lock = config['name']
            self.lockpath = '/'+self.id+'/'+self.lock
            self.locked = False
        except Exception, err:
            self.log('CRIT', 'Failed to init ZKLockServer: '+str(err))
            raise
        else:
            self.log('INFO','ZK create')

    def getlock(self):
        if self.zkclient.write(self.lockpath,gethostname(),1) == 0:
            self.log('INFO', 'lock good')
            self.locked = True
            return True
        else:
            self.log('ERROR', 'lock fail')
            return False

    def setLockName(self, name):
        self.lock = name
        self.lockpath = '/'+self.id+'/'+self.lock

    def releaselock(self):
        if self.zkclient.delete(self.lockpath) ==0:
            self.log('INFO','Success unlock')
            self.locked = False
            return True
        else:
            self.log('ERROR', 'Fail unlock')
            return False

    def checkLock(self):
        try:
            isMyLock = self.zkclient.read(self.lockpath)
        except Exception, err:
            self.log('ERROR', 'lock isnot mine')
            return False
        else:
            return True#isMyLock

    def destroy(self):
        if self.zkclient.disconnect() == 0:
            self.log('INFO','Successfully disconnect from LS')
            return True
        else:
            self.log('ERROR','Cannot disconnect from LS')
            return False

#==========================================
#
#from kazoo.client import KazooClient
#from kazoo.protocol.states import KazooState
#
#class KazooLockServer(BaseLockServer):
#
#    def __init__(self, **config):
#        """
#        "type"  :   "Zookeeper",
#        "app_id":   "Combaine",
#        "name"  :   "combainer_lock",
#        "host"  :   ["cocaine-log01g.kit.yandex.net:2181","cocaine-log02f.kit.yandex.net:2181","cocaine-mongo03f.kit.yandex.net:2181"],
#        "timeout": 5
#        """
#        # def checkConfig
#        try:
#            self.zkclient = KazooClient(hosts=','.join(config["host"]), timeout=config["timeout"])
#            self.id = config['app_id']
#            self.lock = config['name']
#            self.lockpath = '/'+self.id+'/'+self.lock
#            self.locked = False
#            self.zkclient.start()
#        except Exception, err:
#            self.log('CRIT', 'Failed to init ZKLockServer: '+str(err))
#            raise
#        else:
#            self.log('INFO','ZK create')
#
#    def getlock(self):
#        try:
#            self.zkclient.create(path=self.lockpath, value=gethostname(), ephemeral=True, makepath=True)
#        except Exception, err:
#            self.locked = False
#            return False
#        else:
#            self.locked = True
#            return True
#
#    def setLockName(self, name):
#        self.lock = name
#        self.lockpath = '/'+self.id+'/'+self.lock
#
#    def releaselock(self):
#        try:
#            self.zkclient.delete(path=self.lockpath)
#        except Exception, err:
#            return False
#        else:
#            self.locked = False
#            return True
#
#    def checkLock(self):
#        try:
#            isMyLock = self.zkclient.exists(path=self.lockpath)
#        except Exception, err:
#            self.log('ERROR', 'lock isnot mine')
#            return False
#        else:
#            return isMyLock
#
#    def destroy(self):
#        try:
#            self.zkclient.stop()
#        except Exception, err:
#            return False
#        else:
#            return True



def LockServerFactory(**config):
    types = { "Zookeeper"   : ZKLockServer,
              #"Zookeeper"   : KazooLockServer,
    }
    try:
        instance = types[config["type"]](**config)
    except KeyError, errmsg:
        print "Unknown type of lockserver. Type must be one of "+" ".join(types.keys())
    else:
        return instance
    return None
