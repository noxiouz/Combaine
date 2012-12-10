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

import logging
import urllib
import time
import collections
import os
import json
import hashlib
import signal
import sys
import re
#import pprint
import random

from combaine.plugins import LockServerAPI
from combaine.plugins import StorageAPI
#import Receiver
import Observer.client
import Scheduler.scheduler

log = logging.getLogger('combaine')

class Combainer():

    def __init__(self, **config):
        self.aggrhosts = {}
        self.groups_conf = {}
        self.parsing_confs = []
        #-----------------------------------------
        #-----------------------------------------
        observer_conf = config["Observer"] if config.has_key("Observer") else {}
        self.observer = Observer.client.ObserverClient(**observer_conf)
        try:
            self.HTTP_HAND = config['HTTP_HAND']
            self.MIN_PERIOD = config['MINIMUM_PERIOD']
            self.MAX_PERIOD = config['MAXIMUM_PERIOD']
            self.MAX_ATTEMPS = config['MAX_ATTEMPS']
            self.MAX_RESP_WAIT_TIME = config['MAX_RESP_WAIT_TIME']
            self.MAX_PROC_COUNT = config['MAX_PROC_COUNT']
        except Exception, err:
            log.error('__init__() failed: '+str(err))
            raise
    
    def destroy(self):
        """ Destroy combainer safely. I hope) """
        if hasattr(self, "lockserver"):
            try:
                self.lockserver.destroy()
            except Exception, err:
                log.debug('Destroy LS %s' % err)

        if hasattr(self, "storage"):
            try:
                self.storage.destroy()
            except Exception, err:
                log.debug('Destroy ST %s' % err)

        #try:
        #    self.killallinPool()
        #except Exception, err:
        #    log.debug('Destroy Pool %s' % err)
        #    pass

    def getConfigsList(self):
        pattern = '[^.]*\.json$'
        regex = re.compile(pattern)
        log.debug('Read configs')
        PARSING_CONF_PATH='/etc/combaine/parsing'
        AGGREGATE_CONF_PATH='/etc/combaine/aggregate'
        try:
            self.parsing_confs = [filename for filename in os.listdir(PARSING_CONF_PATH) if regex.match(filename)]
            print self.parsing_confs
        except Exception, err:
            log.error('No configs: '+ str(err) )
            print 'ERRORR'+str(err)
            return False 
        else:
            return True

    def prepareMessages(self, time_period):
        """  Make dict of messages for cloud applications.
        PARAMS:
        time_period - (start_time, finish_time) - Describe period of time
        Construct structure of self.messages:

        dict = { "group_name": [set(messages_to_parsing), set(messages_to_aggregate) ],
                "group_name2" : etc
        }

        """
        def getSectionFromConf(confpath, key):
            try:
                values = json.load(open(confpath, 'r'))[key] if key else json.load(open(confpath, 'r'))
            except Exception, err:
                log.error('No section %s in: %s %s' %(key, confpath, str(err)))
                return []
            else:
                return values

        self.messages = {}
        AGGREGATE_CONF_PATH='/etc/combaine/aggregate'
        PARSING_CONF_PATH='/etc/combaine/parsing'
        #---------------------------------------------
        try:
            ParConfs = self.parsing_confs
        except Exception, err:
            log.error('No configs: '+ str(err) )
            return False
        print ParConfs
        #-----------------------------------------------------
        for conf, groups in dict(map(lambda x: (x, getSectionFromConf(PARSING_CONF_PATH + '/' + x, "groups")), ParConfs)).items():
            for group in groups:
                self.groups_conf[group] = self.groups_conf[group] + [conf] if self.groups_conf.has_key(group) else [conf]
        if not self.__getHosts():
            return False

        empty_groups = list(set(self.groups_conf.keys()).difference(set(self.aggrhosts.keys())))
        for group in empty_groups:
            del self.groups_conf[group]

        for group in self.groups_conf.keys():
            self.messages[group] = [set(), set()]
            for host in self.aggrhosts[group]:
                for pars_config in self.groups_conf[group]:
                    self.messages[group][0].add('%(host)s;%(config)s;%(group)s;%(time)s;%(suff)s' % { 'host': host,
                                                                                            'group' : group,
                                                                                           'config': pars_config.rstrip('.json'),
                                                                                           'time':';'.join(time_period),
                                                                                           'suff' : '__H__'} )

        for group, confs in self.groups_conf.items():
            for conf in confs:
                for agg_conf in getSectionFromConf(PARSING_CONF_PATH + '/'+ conf, "agg_configs"):
                    self.messages[group][1].add( "%(group)s;%(confs)s;%(agg_conf)s;%(time)s;%(suff)s" % { 'group' : group,
                                                                                     'confs' : conf.rstrip('.json'),
                                                                                     'agg_conf' : agg_conf,
                                                                                     'time' : ';'.join(time_period),
                                                                                     'suff' : '__G__'} )
        #------------------------------------------------------------------------------------
        self.observer.getCombainerInfo(self.parsing_confs, self.groups_conf.keys())
        #------------------------------------------------------------------------------------
        import pprint
        pprint.pprint(self.messages)
        return True

    def __checkOsSignals(self):
        """ Handler for os signals like a SIGHUP, SIGTERM, SIGINT"""
        def __handleSignals(signum, stack):
            self.destroy()

        signal.signal(signal.SIGHUP, __handleSignals)
        signal.signal(signal.SIGTERM, __handleSignals )
        signal.signal(signal.SIGINT, __handleSignals )

    def __myGroups(self):
        """ Return list of aggregation groups """
        ret = self.groups_conf.keys() if len(self.groups_conf) > 0 else []
        return ret

    def __getHosts(self):
        """ Receive hosts by groupnames from HTTP.
            Groupnames were received from configs.
            ========================================
            TODO:
            Add other methods
        """
        try:
            for group in self.__myGroups():
                aggregatehostfile = urllib.urlopen(self.HTTP_HAND + group).read()
                if aggregatehostfile == 'No groups found':
                    log.warning('Invalid group name: %s' % str(group))
                else:
                    self.aggrhosts[group] = aggregatehostfile.split('\n')[:-1]
        except Exception, err:
            log.error('Failed to load hosts %s' % str(err))
            return False
        else:
            if len(self.aggrhosts.keys()) == 0:
                log.error('There are no hosts in given groups.')
                return False
            else:
                log.debug('Load hosts succesfully')
                return True

    def __buildMessageQueues(self, all_msg_dict, mode="new"):
        """
        if mode == "new":
        all_msgs_dict:
        {   "group_name": [set(messages_to_hosts), set(messages_to_groups)]

        }
        Get from self.prepareMessages()!!!

        if mode == "cont":
        all_msg_dict:
        {
            fullmessage : value
        }
        for example:
        {
            "d67343....67;host1.yandex.net;config-nginx;10;20"  :  'SEND'

        }
        RETURN:
        MessageQueues:
        {
            "hash_key": deque('Group_messages','Host_msg1','Host_msg2','Host_msg3', etc),
            "hash_key2": deque('Group_messages2','Host_msg1','Host_msg2','Host_msg3', etc)
        }
        """
        self.MessageQueues = {}
        if mode == "new":
            for group in all_msg_dict.keys():
                hash_key = hashlib.md5("%d%s%d" % (time.time(), group, random.randint(0, 1000))).hexdigest()
                message_queue = collections.deque()
                message_queue.extend(all_msg_dict[group][1])
                message_queue.append('')
                message_queue.extend(all_msg_dict[group][0])
                self.MessageQueues[hash_key] = message_queue
        #-----------------------------------------------------------------------
        if mode == "cont":
            for record in all_msg_dict.keys():
                hash_key, msg_type, message = record.split(";")[0],record.split(";")[-1], ';'.join(record.split(";")[1:])
                if not self.MessageQueues.has_key(hash_key):
                    self.MessageQueues[hash_key] = collections.deque([''])
                if  msg_type == "__H__":
                    self.MessageQueues[hash_key].append(message)
                elif msg_type == "__G__":
                    self.MessageQueues[hash_key].appendleft(message)

    def __getMsgfromQueue(self, queue_hash_key):
        """ """
        msg = self.MessageQueues[queue_hash_key].pop()
        self.MessageQueues[queue_hash_key].appendleft('')
        return msg;

    def __exportMessageQueuestoList(self):
        """Return list object, based on MessageQueue. """
        ret = []
        for hash_key, msg in self.MessageQueues.items():
            ret = ret + [hash_key + ";" + msg for msg in [x for x in list(msg) if x != ""]]
        return ret

    def createClientPool(self, config):
        """ Create Cocaine Client
        PARAMS:
        config - path to config.json
        RETURN:
        True or False"""
        log.info('Create cocaine.client with config: %s' % config)
        try:
            self._scheduler = Scheduler.scheduler.Scheduler(config)
        except Exception, err:
            log.critical('Cannot create cocaine.client %s' % str(err))
            return False
        else:
            log.debug('Create cocaine.client succesfully %s' % config)
            return True

    def createStorage(self, config):
        """ Create a Storage object based on config dictionary
        PARAMS:
        config - dictionary with params.
        RETURN:
        True or False
        EXAMPLE:
        {   "type": "Zookeeper",
            "app_id": "test_storage",
            "host" : ["cocaine-log01g.kit.yandex.net:2181","cocaine-log02f.kit.yandex.net:2181","cocaine-mongo03f.kit.yandex.net:2181"],
            "timeout" : 5
        }
        """
        try:
            log.debug('Create storage object %s' % str(config))
            #------ Hack
            if len(self.parsing_confs) == 1:
                config["app_id"] = "%s@%s" % (config["app_id"], self.parsing_confs[0].split('.')[0]) # strip .json
            #-----------
            self.storage = StorageAPI.StorageFactory(**config)
        except Exception, err:
            log.error('Cannot create storage object: %s' % str(err))
            return False
        else:
            log.debug('Create storage succesfully')
            return True

    def createLockServer(self, config):
        """ Create a LockServer object based on config dictionary
        PARAMS:
        config - dictionary with params
        RETURN:
        True of False
        EXAMPLE:
        """
        try:
            log.debug('Create LockServer object %s' % str(config))
            self.lockserver = LockServerAPI.LockServerFactory(**config)
        except Exception, err:
            log.error('Cannon create LockServer object: %s' % str(err))
            return False
        else:
            log.debug('Create lockserver succesfully')
            return True

    def getLock(self):
        for config_name in self.parsing_confs:
            log.debug('Get the lock %s' % config_name)
            self.lockserver.setLockName(config_name.split('.')[0]) #strip ext .json
            if self.lockserver.getlock():
                self.parsing_confs = [config_name]
                return True
        return False

    def isMyLock(func):
        def wrapper(self, *args):
            if self.lockserver.checkLock():
                return func(self, *args)
            else:
                raise Exception("Lock is lost")
                log.warn("Lose lock")
                return None # Unaccesable part
        return wrapper

    @isMyLock
    def releaseLock(self):
        log.info('Release the lock')
        return self.lockserver.releaselock()

    @isMyLock
    def __write(self, key, value):
        log.debug('Write a ' + ' '.join((key, value)))
        return self.storage.put(key, value)
    
    @isMyLock
    def __modify(self, key, value):
        log.debug('Modify a ' + ' '.join((key, value)))
        return self.storage.modify(key, value)

    @isMyLock
    def __markFinish(self, value):
        try:
            log.debug('Set up a finishmark with value: %s' % str(value))
            return self.__write('FINISHMARK', value)
        except Exception, err:
            log.error('Error, while setting FINISH: %s' % str(err))
            return False

    @isMyLock
    def __unmarkFinish(self):
        try:
            log.debug('Delete FINISHMARK')
            return self.storage.delete('FINISHMARK')
        except Exception, err:
            log.error('Error, while deleting FINISHMARK')
            return False

    def restoreProgress(self):
        allrecords = self.storage.list()
        if allrecords == None:
            log.error('Cannot read progress data')
            return None
        progress = {}
        for rec in allrecords:
            if rec == 'STARTMARK':
                log.info('Find startmark')
            elif rec == 'FINISHMARK':
                log.debug('Find FINISHMARK')
            else:
                log.info('Find old message '+str(rec))
            info = self.storage.get(rec)
            if info:
                progress[rec] = info
            else:
                return None
        return progress

    def __buildNewSession(self, time_period):
        if not self.prepareMessages(time_period):
            log.debug('Error in self.prepareMessages')
            raise Exception
        self.__buildMessageQueues(self.messages)
        self.__unmarkFinish()
        map(lambda x: self.__write(x, "SEND"), self.__exportMessageQueuestoList(), )

    def __buildContinue(self, progress):
        self.__buildMessageQueues(progress, mode="cont")

    def distribute(self):
        self.msg_count = 0
        #---------------------------------------------------------------------------------
        def defineTimeBorder(progress):
            now_time = str(int(time.time()))
            if progress.has_key('FINISHMARK'):
                log.debug('Start new session. There is a FINISHMARK')
                last_time = progress['FINISHMARK'].split(':')[1]
                sleep_time = int(last_time) - (int(now_time) - self.MIN_PERIOD) + 1
                if sleep_time > 0:
                    log.info('Sleep for minimal period %d'  % sleep_time)
                    time.sleep(sleep_time - 0.1)
                    now_time = str(int(now_time) + sleep_time)
                assert(int(last_time))
            else:
                log.info('No last_time. Take minimal %d' % self.MIN_PERIOD)
                last_time = str(int(now_time) - self.MIN_PERIOD)
            last_time = str(max(int(last_time), int(now_time) - self.MAX_PERIOD))
            # add check of minimum time
            log.debug(' '.join(('End time:', now_time, ' Begin time:', last_time) ) )
            now_time = str(int(now_time) - 1)
            return last_time, now_time

        def __getMsgStripefromQueue(getOne, hash_key):
            def wrapper():
                return getOne(hash_key)
            return wrapper

        def __sendMessagetoCloud(message, app_endpoint, _timeout):
            _message = message.split(';')
            log.debug('%d Send message: %s in queue %d' % (time.time(), message, self.msg_count))
            _toQueue = { "responce" :   None,
                         "app"      :   app_endpoint,
                         "message"  :   ';'.join(_message[1:-1]),
                         "uid"      :   _message[0],
                         "timeout"  :   _timeout, #self.MAX_RESP_WAIT_TIME,
                         "retries"  :   self.MAX_ATTEMPS,
                         "answer"   :   ''
                }
            if not self.lockserver.checkLock():
                raise Exception('Lock is not mine')
            self._scheduler.addTask(_toQueue)
            if not self.storage.delete( message ):
                log.error("Cann't delete %s from storage" % message)

        def __setDeadlineStartPoint(_time):
            self._scheduler.setDeadline(_time)
        #----------------------------------------------------------------------------------
        progress = self.restoreProgress()
        time_period = defineTimeBorder(progress)
        #----------------------------------------------------------------------------------
        if progress.has_key('FINISHMARK'):
            self.__buildNewSession(time_period)
        else:
            self.__buildContinue(progress)
        #---------------------------------------------------------------------------------
        __setDeadlineStartPoint(int(time_period[1])) # <--------- FIX TIME POINT OF SESSION BEGINING
        #---------------------------------------------------------------------------------
        # START PARSING POINT
        #--------------------------------------------------------------------------------
        for hash_key in self.MessageQueues.keys():
            msgs = [hash_key + ";" + item for item in iter(__getMsgStripefromQueue(self.__getMsgfromQueue, hash_key), '')]
            for _msg in msgs:
                __sendMessagetoCloud(_msg, 'parsing/parsing', 0.8*self.MAX_RESP_WAIT_TIME)
        for resp in self._scheduler.schedule():
                self.observer.handleAnswer(resp)
        self.__checkOsSignals()
        #-------------------------------------------------------
        # START AGGREGATING POINT
        #-------------------------------------------------------
        log.debug('DEBUG %i' % self.msg_count)
        self.msg_count = 0
        __setDeadlineStartPoint(int(time.time()))
        for hash_key in self.MessageQueues.keys():
            msgs = [hash_key + ";" + item for item in iter(__getMsgStripefromQueue(self.__getMsgfromQueue, hash_key), '')]
            for _msg in msgs:
                __sendMessagetoCloud(_msg, 'aggregate_group/aggregate_group', int(time_period[1]) + self.MAX_RESP_WAIT_TIME - int(time.time()))
        for resp in self._scheduler.schedule():
                self.observer.handleAnswer(resp)
        self.__checkOsSignals()
        if not self.__markFinish(':'.join(time_period)):
            raise Exception
        #----------------------------------------------------------
        # 
        #----------------------------------------------------------
        self.observer.sendStatistic()
