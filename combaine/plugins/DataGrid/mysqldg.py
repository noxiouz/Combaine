from __abstractdatagrid import AbstractDataGrid

import logging
import os
import random

import MySQLdb
from MySQLdb.cursors import DictCursor
from MySQLdb.cursors import Cursor

class MySqlDG(AbstractDataGrid):

    def __init__(self, **config):
        self.log = logging.getLogger('combaine')
        self.place = None
        self.tablename = ''
        try:
            port = config['local_db_port'] if config.has_key('local_db_port') else 3306 
#            user = config['local_db_user'] if config.has_key('local_db_user') else None
            self.dbname = config['local_db_name'] if config.has_key('local_db_name') else 'COMBAINE'
            self.db = MySQLdb.connect(port=port, user='root')
            cursor = self.db.cursor()
            cursor.execute('CREATE DATABASE IF NOT EXISTS %s' % self.dbname)
            self.db.commit()
            cursor.close()
            self.db.select_db(self.dbname)
        except Exception, err:
            self.log.error('Error in init MySQLdb: %s' % str(err))
            print str(err)
            raise Exception

    def putData(self, data, tablename):
        try:
            tablename = tablename.replace('.','_')
            tablename = tablename.replace('-','_')
            tablename = tablename.replace('+','_')
            table_file = open('/dev/shm/%s-%i' % ('COMBAINE', random.randint(0,65535)) ,'w')
            line = None
            for line in data:
                #print line
                table_file.write(','.join([str(x) for x in line.values()])+'\n')
            table_file.close()

            if not line:
                print 'No data'
                os.remove(table_file.name)
                return True

            if not self._preparePlace(line):
                print "Can't prepare table"
                self.log.error('Unsupported field types. Look at preparePlace()')
                return False

            cursor = self.db.cursor()
            cursor.execute('DROP TABLE IF EXISTS %s' % tablename)
            query = "CREATE TABLE IF NOT EXISTS %(tablename)s %(struct)s ENGINE = MYISAM DATA DIRECTORY='/dev/shm/'" % { 'tablename' : tablename,\
                                                                                                        'struct' : self.place }
            #print query
            cursor.execute(query)
            self.db.commit()

            query = "LOAD DATA INFILE '%(filename)s' INTO TABLE %(tablename)s FIELDS TERMINATED BY ','" % { 'filename' : table_file.name,\
                                                                                                            'tablename': tablename  }
            #print query
            cursor.execute(query)
            self.db.commit()
            cursor.close()
            os.remove(table_file.name)
        except Exception, err:
            self.log.error('Error in putData: %s' % str(err))
            print str(err)
            return False
        else:
            self.tablename = tablename
            return True

    def _preparePlace(self, example):
        ftypes = { type(1) : "INT",
                   type("string")  : "TEXT",
                   type(u"string") : "TEXT",
                   type(1.0)     : "FLOAT"
        }
        #print example.items()
        try:
            self.place = '( %s )' % ','.join([" %s %s" % (field_name, ftypes[type(field_type)]) for field_name, field_type in example.items()])
        except Exception, err:
            self.log.error('Error in preparePlace: %s' % str(err))
            print str(err)
            self.place = None
            return False
        else:
        #    print self.place
            return True

    def perfomCustomQuery(self, query_string):
        #cursor = DictCursor(self.db)
        cursor = Cursor(self.db)
        cursor.execute(query_string)
        _ret = cursor.fetchall()
        cursor.close()
        return _ret

PLUGIN_CLASS = MySqlDG
