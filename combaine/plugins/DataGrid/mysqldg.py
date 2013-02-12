from __abstractdatagrid import AbstractDataGrid

import logging
import os
import random

import MySQLdb
from MySQLdb.cursors import DictCursor
from MySQLdb.cursors import Cursor
from warnings import filterwarnings

#Suppressing warnings
filterwarnings('ignore', category = MySQLdb.Warning)

class MySqlDG(AbstractDataGrid):

    def __init__(self, **config):
        self.log = logging.getLogger('combaine')
        self.place = None
        self.tablename = ''
        try:
            port = config['local_db_port'] if config.has_key('local_db_port') else 3306 
            unix_socket = config['MysqlSocket'] if config.has_key('MysqlSocket') else "/var/run/mysqld/mysqld.sock"
#            user = config['local_db_user'] if config.has_key('local_db_user') else None
            self.dbname = config['local_db_name'] if config.has_key('local_db_name') else 'COMBAINE'
            self.db = MySQLdb.connect(unix_socket=unix_socket, user='root')
            self.cursor = self.db.cursor()
            self.cursor.execute('CREATE DATABASE IF NOT EXISTS %s' % self.dbname)
            self.db.commit()
            #cursor.close()
            self.db.select_db(self.dbname)
        except Exception, err:
            self.log.error('Error in init MySQLdb: %s' % str(err))
            print str(err)
            raise Exception

    def putData(self, data, tablename):
        try:
            tablename = tablename.replace('.','_').replace('-','_').replace('+','_')
            line = None
            with open('/dev/shm/%s-%i' % ('COMBAINE', random.randint(0,65535)) ,'w') as table_file:
                for line in data:
                    table_file.write('GOPA'.join([str(x) for x in line.values()])+'\n')
                table_file.close()

                if not line:
                    print 'No data'
                    os.remove(table_file.name)
                    return False

            if not self._preparePlace(line):
                print "Can't prepare table"
                self.log.error('Unsupported field types. Look at preparePlace()')
                return False

            self.cursor.execute('DROP TABLE IF EXISTS %s' % tablename)
            query = "CREATE TEMPORARY TABLE IF NOT EXISTS %(tablename)s %(struct)s ENGINE = MEMORY DATA DIRECTORY='/dev/shm/'" % { 'tablename' : tablename,\
                                                                                                        'struct' : self.place }
            #print query
            self.cursor.execute(query)
            self.db.commit()

            query = "LOAD DATA INFILE '%(filename)s' INTO TABLE %(tablename)s FIELDS TERMINATED BY 'GOPA'" % { 'filename' : table_file.name,\
                                                                                                            'tablename': tablename  }
            #print query
            self.cursor.execute(query)
            self.db.commit()
            #secursor.close()
            if os.path.isfile(table_file.name):
                os.remove(table_file.name)
        except Exception, err:
            self.log.error('Error in putData: %s' % str(err))
            print str(err)
            if os.path.isfile(table_file.name):
                os.remove(table_file.name)
            return False
        else:
            self.tablename = tablename
            return True

    def _preparePlace(self, example):
        ftypes = { type(1) : "INT",
                   type("string")  : "VARCHAR(2000)",
                   type(u"string") : "VARCHAR(2000)",
                   type(1.0)     : "FLOAT"
        }
        #print example.items()
        try:
            self.place = '( %s, INDEX USING BTREE(TIME))' % ','.join([" %s %s" % (field_name, ftypes[type(field_type)]) for field_name, field_type in example.items()])
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
        #cursor = Cursor(self.db)
        self.cursor.execute(query_string)
        _ret = self.cursor.fetchall()
        self.db.commit()
        #cursor.close()
        return _ret

    def __del__(self):
        if self.db:
            self.log.info("Destruct me! YeeaaaHHH!!!")
            print "Called a destructor"
            self.cursor.close()
            self.db.commit()
            self.db.close()
            print "Success"

PLUGIN_CLASS = MySqlDG
