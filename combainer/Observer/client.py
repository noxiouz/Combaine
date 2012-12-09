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


#import pprint
import os
import socket

import zmq

# Decorators

def decorator_maker_for_handleAnswer():

    def decorator( func ):
        def wrapper(self, msg):
            try:
                result_status = msg[0].split(';')[0]
                from_hostname = msg[0].split(';')[-2]
                if result_status == "success":
                    self.stat_dict["Success"] += 1
                    if self.stat_dict["lasterrors"].has_key(from_hostname):
                        self.stat_dict["lasterrors"].pop(from_hostname)

                elif result_status == "failed":
                    self.stat_dict["Failed"] += 1
                    error_text = msg[0].split(';')[-1]
                    self.lasterrors_dict[from_hostname] = error_text
                    self.stat_dict["lasterrors"] = self.lasterrors_dict

                self.stat_dict["Answer"] += 1
            except Exception, err:
                pass
            func(self, msg)
        return wrapper
    return decorator

def getSystemInfo():
    sysinfo = { "hostname"  :   socket.gethostname(),
                "PID"       :   os.getpid()
    }
    return sysinfo

#-------------------------------------

class ObserverClient():
    
    def __init__(self, **conf):
        """
        conf:
        {
            "host"  :   "hostname:port"
        }
        """
        self.stat_dict = {
                            "ProcInfo"  : getSystemInfo(),
                            "Success"   :   0,
                            "Failed"    :   0,
                            "Sessions"  :   0,
                            "lasterrors" :  {}
                        }
        self.lasterrors_dict = {}
        if conf.has_key("host"):
            try:
                host = conf["host"].split(':')[0]
                port = int(conf["host"].split(':')[1])
                self.createSender(host, port)
            except Exception, err:
                def dummy():
#                    print "I'm dummy"
                    pass
                self.sendStatistic = dummy


    def createSender(self, host, port):
        self.__context = zmq.Context()
        self.socket = self.__context.socket(zmq.PUSH)
        self.socket.connect("tcp://%s:%d" % (host, port))
        self.socket.setsockopt(zmq.HWM, 1)
        self.socket.setsockopt(zmq.LINGER, 100)

    def getCombainerInfo(self, config_names_list, group_names_list):
        self.stat_dict["CombainerInfo"] = { "Serviced config"   : ','.join(config_names_list),
                                            "Serviced groups"   : ','.join(group_names_list)
        }

    def handleMessage(self, msg):
        print msg

    @decorator_maker_for_handleAnswer()
    def handleAnswer(self, answer):
        pass

    def sendStatistic(self):
        #pprint.pprint( self.stat_dict )
        self.lasterrors_dict = {}
        try:
            self.socket.send_json(self.stat_dict, zmq.NOBLOCK)
        except:
            pass

