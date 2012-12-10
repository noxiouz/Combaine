#! /usr/bin/env python

import unittest

import sys, os

sys.path[0] = '/'.join(os.path.abspath('.').split('/')[:-1])


class import_test(unittest.TestCase):

    def test_combaine(self):
        try:
            from combaine.combainer import combainer 
        except Exception, err:
            #print err
            res = False
        else:
            res = True
        finally:
            self.assertTrue(res)

    def test_lockserver(self):
        try:
            from combaine.plugins import LockServerAPI
        except Exception, err:
            print err
            res = False
        else:
            res = True
        finally:
            self.assertTrue(res)



if __name__ == "__main__":
    unittest.main()
