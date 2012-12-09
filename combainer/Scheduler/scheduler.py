#
#
#
#

import collections
import logging
import time

import cocaine.client

log = logging.getLogger('combaine')

def coroutine(func):
    def wrapper(*args,**kwargs):
        cr = func(*args,**kwargs)
        cr.next()
        return cr
    return wrapper


class Scheduler(object):

    def __init__(self, config):
        self.deadline = 0
        self.queue = collections.deque()
        self.answers = []
        self.__initCLient(config)

    def __initCLient(self, config):
        try:
            log.debug('Init client:')
            self.cocaine_client = cocaine.client.Client(config)
        except Exception, err:
            log.error('Error. Cannot create cocaine client: '+str(err))
            return False
        else:
            return True

    def addTask(self, taskstruct):
        """
        taskstruct = {            "responce"  : resp,
                                "app"       : task["app"],
                                "message"   : task["message"],
                                "uid"       : task["uid"],
                                "timeout"   : task["timeout"],
                                "retries"   : task["retries"] - 1,
                                "answer"    : ''
                         }
        """
        # Actualize timeout value
        actual_timeout = taskstruct["timeout"] + self.deadline - int(time.time())
        # Decrease attemp count
        taskstruct['retries'] -= 1
        if actual_timeout > 0 and taskstruct['retries'] >= 0:
            taskstruct["timeout"] = actual_timeout
            #taskstruct['answer'] = ''
            log.info("SEND: %s %s %s %d %d" % (taskstruct["app"], taskstruct["message"], taskstruct["uid"],\
                                                                        taskstruct["timeout"], taskstruct['retries']))
            taskstruct['responce'] = self.cocaine_client.send(taskstruct["app"], taskstruct["message"],\
                    deadline=taskstruct["timeout"], timeout=0)
            self.queue.appendleft(taskstruct)
        else:
            self.answers.append((taskstruct['answer'], taskstruct['uid']))

    def setDeadline(self, deadline):
        self.deadline = deadline

    def isReachedDeadline(self):
        return self.deadline > int(time.time())

    def _resendFilter(self, answer_text):
        try:
            ans = True if (answer_text.split(';')[0] == "failed") else False
        except:
            return False
        else:
            return ans

    @coroutine
    def _doTask(self):
        ans = None
        while True:
            resp = (yield ans)
            try:
                ans = resp.get(timeout=1)
            except RuntimeError, err:
                ans = 'failed;RuntimeCocaineMsg: %s' % err

    def schedule(self):
        self.answers = []
        while len(self.queue) > 0: 
            if self.isReachedDeadline():
                log.debug('Deadline in sheduler. Clear queue')
                self.queue.clear()
                return
            taskstruct = self.queue.pop()
            ans = self._doTask().send(taskstruct['responce'])
            print 'DDDDDDDDDDDD ' + ans
            if ans != '':
                taskstruct['answer'] = taskstruct['answer'] + ans
                if self._resendFilter(taskstruct['answer']):
                    self.addTask(taskstruct)
                else:
                    self.answers.append((taskstruct['answer'], taskstruct['uid']))
            else:
                self.queue.appendleft(taskstruct)
        for ans in self.answers:
            log.info(ans)
        return self.answers
