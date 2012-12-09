#! /usr/bin/env python

import DataGrid.datagrid as dg


config = {}

d = dg.MySqlDG(**config)

info = { 'i' : 'INTEGER',
        'name' : 'STRING'
    }

#d.preparePlace(info)

l =[]
for i in range(0,1000):
    l.append([str(i),'TEST'])
d.putData(l,'TEST_TABLE')

query="SELECT * FROM TEST_TABLE"
print d.perfomCustomQuery(query)
