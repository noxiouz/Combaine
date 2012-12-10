from abc import ABCMeta, abstractmethod


class BaseLockServer(object):
    """ """
    __metaclass__ = ABCMeta

    @abstractmethod
    def getlock(self):
        raise NotImplementedError

    @abstractmethod
    def setLockName(self):
        raise NotImplementedError
    
    @abstractmethod
    def releaselock(self):
        raise NotImplementedError

    @abstractmethod
    def destroy(self):
        raise NotImplementedError

    def log(self, level, message):
        logger.debug(message)

    @abstractmethod
    def checkLock(self):
        raise NotImplementedError

    @abstractmethod
    def destroy(self):
        raise NotImplementedError
