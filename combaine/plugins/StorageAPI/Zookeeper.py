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

from combaine.common.ZKeeperAPI import zkapi as ZK
from __abstractstorage import BaseStorage

import logging

#-------------------------------------------------------------
# TEST VERSION!!!
#-------------------------------------------------------------

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

PLUGIN_CLASS = ZKStorage
