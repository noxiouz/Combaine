from __abstractstorage import BaseStorage

class NullStorage(BasicStorage):

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

PLUGIN_CLASS = NullStorage
