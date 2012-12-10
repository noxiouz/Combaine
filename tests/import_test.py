#! /usr/bin/env python

import unittest

import sys, os
sys.path[0] = '/'.join(os.path.abspath('.').split('/')[:-1])

class import_test(unittest.TestCase):

    def test_combaine(self):
        try:
            from combaine.combainer import combainer
        except Exception, err:
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

    def test_storage(self):
        try:
            from combaine.plugins import StorageAPI
        except Exception, err:
            print err
            res = False
        else:
            res = True
        finally:
            self.assertTrue(res)

    def test_fetcher(self):
        try:
            from combaine.plugins import DataFetcher 
        except Exception, err:
            print err
            res = False
        else:
            res = True
        finally:
            self.assertTrue(res)

    def test_datagrid(self):
        try:
            from combaine.plugins import DataGrid 
        except Exception, err:
            print err
            res = False
        else:
            res = True
        finally:
            self.assertTrue(res)

if __name__ == "__main__":
    unittest.main()
