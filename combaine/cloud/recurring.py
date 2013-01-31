# -*- coding: utf-8 -*-
# Да, я буду писать комментарии на русском.

from cocaine.decorators import timer
import time, httplib, logging, os, hashlib, re
import common
from combaine.plugins import LockServerAPI

global global_config
global_config = common.generate_cloud_config()

# Очистка локальной БД
#@timer
#def drop_local_databases():
#	try:
#		log = logging.getLogger(global_config['logger_name'])
#		deadline = int(time.time()) - global_config['local_db_save_time']
#		connection = common.mongo_connect_local()
#		for i in connection.database_names():
#			if i[:9] == global_config['db_prefix']:
#				db = connection[i]
#				for coll_name in db.collection_names():
#					if coll_name == 'system.indexes':
#						continue
#					coll = db[coll_name]
#					coll.remove({'time': {'$lt': deadline}})
#				log.info('Database ' + i + ' has been successfully cleared in local MongoDB.')
#		connection.disconnect()
#		return 'success'
#	except Exception as err:
#		log.error(str(err) + ' while trying to clear local database.', exc_info=1)
#		return 'failed'


# Очищение БД replica set
@timer
def clear_remote_db():
	try:
		zk_clear_config = global_config['zk_config']
		zk_clear_config['app_id'] = 'zk_clear_db'
		zk_clear_config['name'] = 'clear_db_lock'
		lock_server = LockServerAPI.LockServerFactory(**zk_clear_config)
		if not lock_server.getlock():
			log.info('Could not get lock while trying to clear remote DBs. Terminating.')
			lock_server.destroy()
			return 1
		# Очищение replica set'а, указанного в глобальном конфиге
		global_mongo_hosts = global_config['mongo_hosts']
		sub_clear_remote_db(global_config)
		# Очищение relica set'ов, указанных в конфигах парсинга
		for filename in os.listdir('/etc/combaine/parsing'):
			pattern = '[^.]*\.json$'
			regex = re.compile(pattern)
			if regex.match(filename):
				config_name = filename[:-5]
				config = common.generate_parsing_config(config_name)
				if config['mongo_hosts'] == global_mongo_hosts:
					continue
				else:
					sub_clear_remote_db(config)
		if not lock_server.releaselock():
			log.error('Could not release lock after clearing remote DBs.')
			lock_server.destroy()
			return 1
		lock_server.destroy()
		return 0
	except Exception as err:
		log.error(str(err) + ' while trying to clear remote database.', exc_info=1)
		lock_server.destroy()
		return 1


# Отправка данных agave
@timer
def agave_send():
	try:
		log = logging.getLogger(global_config['logger_name'])
		global_mongo_hosts = global_config['mongo_hosts']
		# Отправляем данные из replica set, указанного в глобальном конфиге
		sub_agave_send(global_config)
		# Отправляем данные из replica set'ов, указанных в локальных конфигах парсинга
		for filename in os.listdir('/etc/combaine/parsing'):
			pattern = '[^.]*\.json$'
			regex = re.compile(pattern)
			if regex.match(filename):
				config_name = filename[:-5]
				config = common.generate_parsing_config(config_name)
				if config['mongo_hosts'] == global_mongo_hosts:
					continue
				else:
					sub_agave_send(config)
	except Exception as err:
		log.error(str(err) + ' while trying to send data to agave.', exc_info=1)

## Формирование списка графиков для CEM
@timer
def cem_create_list():
	try:
		log = logging.getLogger(global_config['logger_name'])
		zk_cem_config = global_config['zk_config']
		zk_cem_config['app_id'] = 'zk_cem_list'
		zk_cem_config['name'] = 'cem_create_list'
		lock_server = LockServerAPI.LockServerFactory(**zk_cem_config)
		if not lock_server.getlock():
			log.info('Could not get lock while trying to clear remote DBs. Terminating.')
			lock_server.destroy()
			return 1

		global_mongo_hosts = global_config['mongo_hosts']
		connection = common.mongo_connect_replicaset(global_mongo_hosts)
		db = connection[global_config['db_prefix']+'res']
		itog = []
		graph_dict = {}
		for collname in db.collection_names():
			if collname[-3:] == '_rt':
				graph = collname[:-3]
				coll = db[collname]
				groups = coll.distinct('group')
				for group in groups:
					fields = coll.find({'group': group}).sort('time', -1).limit(1)[0]['data'].keys()
					itog.append({'_id': hashlib.md5(group + ':' + graph).hexdigest(), 'group': group, 'graph': graph, 'fields': fields})
		coll = db['cem_graphs']
		for line in itog:
			print(line)
			coll.save(line)

		if not lock_server.releaselock():
			log.error('Could not release lock after creating list for CEM')
			lock_server.destroy()
			return 1
		lock_server.destroy()
		return 0
	except Exception as err:
		log.error(str(err) + ' while trying to create list for CEM', exc_info=1)
		lock_server.destroy()
		return 1

def sub_clear_remote_db(config):
	try:
		log = logging.getLogger(config['logger_name'])
		deadline = int(time.time()) - config['remote_db_save_time']
		connection = common.mongo_connect_replicaset(config['mongo_hosts'])
		db = connection[config['db_prefix']+'res']
		for coll_name in db.collection_names():
			if coll_name == 'system.indexes':
				continue
			coll = db[coll_name]
			coll.remove({'time': {'$lt': deadline}})
			log.info('Collection ' + coll_name + ' has been successfully cleared in replica set.')
	
		db = connection[config['db_prefix']+'mid']
		for coll_name in db.collection_names():
			if coll_name == 'system.indexes':
				continue
			coll = db[coll_name]
			coll.remove({'time': {'$lt': deadline}})
			log.info('Collection ' + coll_name + ' has been successfully cleared in replica set.')
		connection.disconnect()
	except Exception as err:
		log.error(str(err) + ' while trying to clear remote db', exc_info=1)


def sub_agave_send(config):
	try:
		log = logging.getLogger(config['logger_name'])
		now = int(time.time())
		limit = now - config['agave_save_time']
		connection = common.mongo_connect_replicaset(config['mongo_hosts'])
		db = connection['agave_spool']
		coll2 = db['log']
		headers = config['agave_headers']
		limit_old = now - config['agave_spool_save']
		for coll_name in db.collection_names():
			if 'system' in coll_name or coll_name=='log':
				continue
			coll = db[coll_name]

			zk_agave_config = config['zk_config']
			zk_agave_config['name'] = coll_name
			zk_agave_config['app_id'] = 'zk_agave'
			lock_server = LockServerAPI.LockServerFactory(**zk_agave_config)
			if not lock_server.getlock():
				log.info('Could not get lock while trying to send ' + coll_name +  ' data to agave. Terminating.')
				lock_server.destroy()
				continue

			success_count = 0
			rejected_count = 0
			deleted_count = 0
			for i in coll.find({'time': {'$lt': limit}}).sort('time', 1):
				if i['time'] < limit_old:
					coll.remove(i)
					deleted_count += 1
					log.warning('Dot ' + i['url'] + ' deleted from spool because it is very old.')
					continue

				for agave_host in config['agave_hosts']:
					headers['Host'] = agave_host+':80'
					conn = httplib.HTTPConnection(agave_host)
					conn.request("GET", i['url'], None, headers)
					resp = conn.getresponse().read()
					conn.close()
					if resp.splitlines()[0] == 'Ok':
						success_count += 1
						coll2.save({'url': i['url'], 'time': i['time'], 'status': 'accepted', 'host': agave_host, 'when': str(time.time()), 'graph': coll_name})
					else:
						rejected_count += 1
						coll2.save({'url': i['url'], 'time': i['time'], 'status': 'rejected', 'host': agave_host, 'when': str(time.time()), 'graph': coll_name})
						log.debug(resp.splitlines()[0] + ' ' + i['url'])

				coll.remove(i)
				deleted_count += 1

			if success_count > 0 or deleted_count > 0 or rejected_count > 0:
				log.info(str(success_count/2) + ' dots have been accepted, ' + str(rejected_count/2) + ' rejected, ' + str(deleted_count) + ' deleted for graph ' + coll_name + '.')

			if not lock_server.releaselock():
				log.error('Could not release lock after sending ' + coll_name + ' data to agave.')
				lock_server.destroy()
				return 1
			lock_server.destroy()
	
		connection.close()
	except Exception as err:
		log.error(str(err) + ' while trying to send data to agave.', exc_info=1)
		lock_server.destroy()

#if __name__ == '__main__':
#	cem_create_list()
