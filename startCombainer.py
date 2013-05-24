#! /usr/bin/env python
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

import logging 
import time 
import sys
import json

from combaine.combainer import combainer
from combaine.common import configlog
from combaine.common.configloader import parse_common_cfg


log = logging.getLogger('combaine')

DEFAULT_STORAGE = {
        "type" : "null",
        "app_id" : "Combaine"
}

def parseConfig():
    combaine_json = parse_common_cfg("combaine")["Combainer"]
    combainer_dict = combaine_json["Main"]
    lockserver_dict = combaine_json["Lockserver"]
    storage_dict = combaine_json.get("Storage", DEFAULT_STORAGE)
    return combainer_dict, lockserver_dict, storage_dict

def Main():
    combainer_dict, lockserver_dict, storage_dict = parseConfig()
    try:
        log.debug('Start combainer')
        cl = combainer.Combainer(**combainer_dict)
        if not cl.getConfigsList():
            log.debug('There is no configs in /etc/combaine/parsing')
            raise Exception("There is no configs")
        if not cl.createLockServer(lockserver_dict):
            log.debug('Cannot create lockserver')
            raise Exception('Cannot create LockServer')
        if not cl.getLock():
            log.debug('Cannot acquire any lock')
            raise Exception('Cannot acquire lock')
        if not cl.createStorage(storage_dict):
            raise Exception
        if not cl.createClientPool('/etc/combaine/combainer.json'):
            raise Exception
        while True:
            cl.distribute()
    except Exception as err:
        log.debug('Try to destroy combainer.'+str(err))
        print str(err)
        try:
            cl.destroy()
        except:
            log.debug('Destroy FAILED ')
        else:
            log.debug('Destroy succesfully')

if __name__ == "__main__":
    Main()
    sys.exit(1)
