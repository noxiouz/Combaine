#! /usr/bin/env python

import Tokenparser
import hashlib
import time
import sys

sys.path = sys.path+['/usr/lib/yandex/combaine/']

def cor(f):
    print 'AAAAAAAAAAAAAAAAAAAAAAAaaa'
    g = f()
    g.next()
    return g.send

@cor
def parse():
    voc={}
    p = Tokenparser.TokenParser()
	#date
    p.skip('[')
    p.upTo('date',' ')
    p.skip(' ')
	# UTC
    p.upTo('utc',']')
    p.skip(']')
    p.skip(' ')
	#host
    p.upTo('host',' ')
    p.skip(' ')
	#source
    p.upTo('source',' ')
    p.skip(' ')
	#method
    p.fromTo('method','"','"')
	#http_status
    p.fromTo('http_status',' ',' ')
	#referer
    p.fromTo('referer','"','"')
	#user_agent
    p.skip(' ')
    p.fromTo('user_agent','"','"')
	#source
    p.skip(' ')
    p.upTo('source',' ')
	#request time
    p.skip(' ')
    p.upTo('request_time',' ')
	#hit/miss
    p.skip(' ')
    p.upTo('status', ' ')
	#bytes
    p.fromTo('bytes_sent',' ','\n')
    #print success
    #for i in p.matches():
    voc ={}
    while True:
        string = (yield voc)
        voc ={}
        success = p.Parse(string)
        voc['_id'] = hashlib.sha256(string).hexdigest()
        voc = p.matches()
        try:
            voc['Time'] = int(time.mktime(time.strptime(voc['date'], "%d/%b/%Y:%H:%M:%S")))
        except ValueError:
            voc['Time'] = 0
        try:
            voc['request_time'] = int(float(voc['request_time'])*1000)
        except ValueError:
            voc['request_time'] = 0
        try:
            voc['method'] = voc['method'].decode('UTF-8')
        except UnicodeDecodeError:
            voc.pop('method')
        try:
            voc.pop('date')
            voc.pop('utc')
            voc.pop('referer')
            voc['bytes_sent'] = int(voc['bytes_sent'])
            voc['http_status'] = int(voc['http_status'])
        except:
            pass
        p.clearMatches()
        #print '111111 %s' % voc
	#return voc

import parsers.parsers
z = parsers.parsers.nginx_access_feeds_parser


f = open('test.log','r')
g = open('test.log','r')
p = parse
#i = []
for l in f:
    a = p(l)
    #print a