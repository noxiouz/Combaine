#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Да, я буду писать комментарии на русском.

from pymongo import Connection
import logging, sys, os, json, collections, time

global config

def convert_unicode_to_str(data):
	try:
		if isinstance(data, unicode):
			return str(data)
		elif isinstance(data, collections.Mapping):
			return dict(map(convert_unicode_to_str, data.iteritems()))
		elif isinstance(data, collections.Iterable):
			return type(data)(map(convert_unicode_to_str, data))
		else:
			return data
	except Exception as err:
		print(err)

def generate_aggregation_config(config_names):
	try:
		log = logging.getLogger(config['logger_name'])
		agg_config = {}
		for config_name in config_names:
			try:
				temp = convert_unicode_to_str(json.load(open('/etc/combaine/aggregate/' + config_name + '.json', 'r')))
				_ = temp.pop('graph_name')
				agg_config[_] = temp
			except ValueError as err:
				log.error('Error in /etc/combaine/aggregate/' + str(config_name) + '.json: ' + str(err))
		return agg_config
	except Exception as err:
		log.error('Error in /etc/combaine/aggregate/' + str(config_name) + '.json: ' + str(err), exc_info=1)

def generate_aggregation_config_single(config_name):
	try:
		log = logging.getLogger(config['logger_name'])
		agg_config = {}
		try:
			temp = convert_unicode_to_str(json.load(open('/etc/combaine/aggregate/' + config_name + '.json', 'r')))
			_ = temp.pop('graph_name')
			agg_config[_] = temp
		except ValueError as err:
			log.error('Error in /etc/combaine/aggregate/' + str(config_name) + '.json: ' + str(err))
		return agg_config
	except Exception as err:
		log.error('Error in /etc/combaine/aggregate/' + str(config_name) + '.json: ' + str(err), exc_info=1)


def generate_cloud_config():
	try:
		global_config = convert_unicode_to_str(json.load(open('/etc/combaine/combaine.json', 'r')))
		return global_config['cloud_config']
	except Exception as err:
		f = open('/var/log/combaine/cloud.log', 'a')
		f.write('ERROR      ' + time.ctime() + ' Error in global config /etc/combaine/combaine.json: ' + str(err) + '.\n')
		f.close()

def generate_parsing_config(config_name):
	try:
		cloud_config = generate_cloud_config()
		log = logging.getLogger(cloud_config['logger_name'])
		try:
			parsing_config = convert_unicode_to_str(json.load(open('/etc/combaine/parsing/' + config_name + '.json', 'r')))
			cloud_config.update(parsing_config)
		except ValueError as err:
			log.error('Error in /etc/combaine/parsing/' + str(config_name) + '.json: ' + str(err))
		return cloud_config
	except Exception as err:
		log.error('Error in /etc/combaine/parsing/' + str(config_name) + '.json: ' + str(err), exc_info=1)


def mongo_connect_replicaset(mongo_hosts):
	try:
		log = logging.getLogger(config['logger_name'])
		connection = Connection(mongo_hosts, fsync=True)
		return connection
	except:
		#print("Problem with connection to replica set")
		log.error("Problem with connection to replica set")

def mongo_connect_local():
	try:
		log = logging.getLogger(config['logger_name'])
		port = str(config['local_mongodb_port'])
		connection = Connection('127.0.0.1:' + port, fsync=True)
		return connection
	except:
		#print("Problem with connection to local mongodb")
		log.error("Problem with connection to local mongodb")

config = generate_cloud_config()
_format = logging.Formatter("%(levelname)-10s %(asctime)s %(message)s")
app_log = logging.getLogger(config['logger_name'])

log_level = eval('logging.' + config['log_level'])

crit_handler = logging.FileHandler('/var/log/combaine/cloud.log')
#crit_handler = logging.StreamHandler(sys.stdout)
crit_handler.setFormatter(_format)
crit_handler.setLevel(log_level)

app_log.addHandler(crit_handler)
app_log.setLevel(log_level)

if __name__ == '__main__':
	print generate_aggregation_config(['http_ok', 'http_ok_timings'])
	print generate_parsing_config('feeds_nginx')
