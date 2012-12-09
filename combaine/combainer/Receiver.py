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

import cocaine.client
import multiprocessing, logging, threading, time, Queue, collections, sys

log = logging.getLogger('combaine')

class Receiver(threading.Thread):

    def __init__(self, config, task_queue, result_queue, for_deadline_timer):
        super(Receiver, self).__init__()
        self._config = config
        self._task_queue = task_queue
        self._result_queue = result_queue
        self._deadline_timer = for_deadline_timer
        self._responces = collections.deque()
        self._stop_event = threading.Event()

    def __ResendFilter(self, answer_text):
        """ Make a decision to resend msg
        Return TRUE for resend
        """
        try:
            ans = True if (answer_text.split(';')[0] == "failed") else False
        except:
            return False
        else:
            return ans

    def stop(self):
        self._stop_event.set()


    def __initCLient(self, config):
        try:
            log.debug('Init client:')
            self.cocaine_client = cocaine.client.Client(config)
        except Exception, err:
            log.error('Error. Cannot create cocaine client: '+str(err))
            return False
        else:
            return True

    def __handleTask(self, task):
        """
        Composes and sends the message in cocaine cloud. When it modifies a service fields.
        Returns a structure for processing the response.

        Task structure:
        list
        [0] - '/'.join(("application_name", "endpoint_name"))
        [1] - message
        [2] - uid
        [3] - deadlinetimer
        [4] - attemps
        """
        try:
            if task["retries"] > 0:
                # Recelculate timeout value 
                new_timeout = task["timeout"] + self._deadline_timer.value - int(time.time())
                # If timeout < 0 it's same as infinity wait. Give last chance for parsing. Is it optimal strategy?? May be 1/2,1/4,/1/8?
                task['timeout'] = new_timeout if new_timeout > 0 else 1
                log.info("SEND: %s %s %s %s %d" % (task["app"], task["message"], task["uid"], task["timeout"], task["retries"]))
                resp = self.cocaine_client.send( task["app"], task["message"], deadline = task["timeout"], timeout = 0 )
                responce = {    "responce"  : resp,
                                "app"       : task["app"],
                                "message"   : task["message"],
                                "uid"       : task["uid"],
                                "timeout"   : task["timeout"],
                                "retries"   : task["retries"] - 1,
                                "answer"    : task["answer"]
                         }
                self._responces.append(responce)
            else:
                log.debug('Droped by attemps')
                self._result_queue.put((task["answer"], task["uid"] ))
        except Exception, err:
            log.error('Some error: '+str(err))
            pass

    def __handleMessage(self):
        """
        self._messageHandler_dict:
        look _handleTask()
        """
        isNext = True
        while isNext:
            isNext = False
            if len(self._responces) < 1:
                return
        #-----------------------------------------
            responce = self._responces.pop()
            try:
                ans = responce['responce'].get(timeout=0.5)
                if ans != '':
                    responce['answer'] = ans +" "+ responce['answer']
                    if self.__ResendFilter(responce["answer"]):
                        self.__handleTask( responce )
                    else:
                        self._result_queue.put((responce["answer"], responce["uid"] ))
                    isNext = True
                else:
                    self._responces.appendleft(responce)
            except RuntimeError, err:
                self._result_queue.put(("CocaineRuntimeMsg: %s %s" % (str(err), responce["answer"]), responce["uid"]))
                isNext = True

    def run(self):
        if not self.__initCLient(self._config):
            self.stop()
        while not self._stop_event.isSet():
            try:
                task = self._task_queue.get(True, 0.5)
                self.__handleTask(task) #<-------- construct responce_dict, Check task
                self._task_queue.task_done()
            except Queue.Empty, err:
                self.__handleMessage()

#------------------------------------------------------------------

#class StoppableThread(threading.Thread):
#    """ U can try to stop this thread by calling stop()
#        Redefine do() for ur function
#    """
#    def __init__(self, func, count, *args):
#        """
#        PARAMS:
#            count - number of attemps. If None - infinity
#        """
#        super(StoppableThread, self).__init__()
#        self.stop_event = threading.Event()
#        self.args = args
#        self.count = count
#        try:
#            assert(callable(func))
#        except Exception, err:
#            log.error('func parametr is not callable')
#        else:
#            self.do = func
#
#    def stop(self):
#        self.stop_event.set()
#
#    def run(self):
#        if not self.count:
#            while not self.stop_event.isSet():
#                if self.do(*self.args):
#                    break
#        else:
#            while (not self.stop_event.isSet()) and (self.count > 0):
#                self.count -= 1
#                if self.do(*self.args):
#                    break
#
#----------------------------------------------------------------------


