#! /usr/bin/env python

import unittest

import MongoReplicaSet


config = { "hosts" : ["cocaine-mongo01g.kit.yandex.net:27017", "cocaine-mongo02g.kit.yandex.net:27017", "cocaine-mongo03f.kit.yandex.net:27017"]
}

test_namespace = 'combaine_mid/test_namespace'

test_data = [
        {'ONE' : 'ONE1',
         'TWO' : 'TWO'},

        {'ONE111' : 'ONE1',
         'TWO111' : 'TWO'}
]

class MongoRS_BasicTest(unittest.TestCase):

    def setUp(self):
        self.db = MongoReplicaSet.MongoReplicaSet(**config)

    def tearDown(self):
        del self.db

class connectionTest(MongoRS_BasicTest):

    def runTest(self):
        self.assertTrue(self.db.connect(test_namespace))

class close_connectionTest(MongoRS_BasicTest):

    def runTest(self):
        self.assertTrue(self.db.connect(test_namespace))
        self.assertTrue(self.db.close())

#==========================================================

class MongoRS_FunctionalTest(unittest.TestCase):

    def setUp(self):
        self.db = MongoReplicaSet.MongoReplicaSet(**config)
        self.db.connect(test_namespace)

    def tearDown(self):
        self.db.close()
        del self.db

class insertTest(MongoRS_FunctionalTest):

    def runTest(self):
        self.assertTrue(self.db.insert(test_data))

def suite_conn_and_close():
    suite = unittest.TestSuite()
    suite.addTest(connectionTest())
    suite.addTest(close_connectionTest())
    return suite

def suite_read_write():
    suite = unittest.TestSuite()
    suite.addTest(insertTest())
    return suite


if __name__ == "__main__":
    runner = unittest.TextTestRunner(verbosity=2)
    print 'Starting base connection tests:'
    runner.run(suite_conn_and_close())
    print 'Starting read/write tests:'
    runner.run(suite_read_write())


