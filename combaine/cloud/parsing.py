# -*- coding: utf-8 -*-

import os, sys, time, pymongo, bson, socket, re, logging, httplib, hashlib
from pymongo import Connection, errors
import common

sys.path = sys.path+['/usr/lib/yandex/combaine/']

from parsers import PARSERS

""" This is the main parsing function """
def statbox_main(host_name, config_name, group_name, previous_time, current_time):
	try:
		global config
		config = common.generate_parsing_config(config_name)
		if config is None or len(config) == 0:
			return 'failed', 'Could not read or parse config'

		log = logging.getLogger(config['logger_name'])
		log.debug(host_name + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': Parsing config is ' + str(config))
		start_time = int(time.time())
		hostname_converted = host_name.replace(".", "_").replace("-","_")

		connection = common.mongo_connect_local()
		if connection is None:
			log.error(host_name + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': Could not connect to local MongoDB.')
			return 'failed', 'Could not connect to local MongoDB'
		log.debug(host_name + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': Connection to local MongoDB is established.')

		log_format = config['parser']
		log.debug(host_name + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': We are going to use ' + log_format + ' parser.')
		global log_format_converted
		log_format_converted = log_format.replace(".", "_").replace("-","_")
		db = connection[config['db_prefix'] + config_name + '_' + group_name]
		log.debug(host_name + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': We are going to use ' + db.name + ' DB.')
		coll = db[hostname_converted]
		log.debug(host_name + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': We are going to use ' + coll.full_name  + ' collection.')

		if coll.count() != 0:
#			log.warning(host_name + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': There were some previous data in ' + str(coll.full_name) + '. Removed it.')
			db.drop_collection(coll)

		if config['get_data_from'] == 'timetail':
			seconds = start_time - int(previous_time) + 1
			log_name = config['log_name']
			log.debug(host_name + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': We are going to take logs for the last ' + str(seconds) + ' seconds from ' + log_name + ' log.')
			port = config['timetail_port']
			url = config['timetail_url'] + log_name + '&time=' + str(seconds)
		elif config['get_data_from'] == 'json':
			url = config['json_url']
			port = config['json_port']
		else:
			log.error(host_name + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': I do not know how to take data from ' + str(config['get_data_from']) + '.')
			return 'failed', 'I do not know how to take data from ' + str(config['get_data_from']) + '.'
		log.debug(host_name + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': We are going to take logs from port ' + str(port) + ' at url: ' + url + '.')

		conn = httplib.HTTPConnection(host_name, port, timeout=5)
		conn.request("GET", url, None)
		resp = conn.getresponse()
		if resp.status == 200:
			datachunk = resp.read()
			conn.close()
			if len(datachunk) == 0:
				log.warning(host_name + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': We have got null data from timetail. Nothing to do. Exiting.')
				time.sleep(1)
				return 'failed', 'I have got null data from ' + host_name + ' (' + str(previous_time) + ' - ' + str(start_time) + ')'
			else:
				log.debug(host_name + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': We have successfully got some logs from timetail (' + str(len(datachunk)) + ' bytes).')
		else:
			conn.close()
			log.error(host_name + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': Could not get logs from host with status ' + str(resp.status) + '.')
			return 'failed', 'Could not get logs from ' + host_name + ' with status ' + str(resp.status)

		if config['get_data_from'] == 'timetail':
			pieces = sys.getsizeof(datachunk)/config['chunk_max_size'] + 1
			data_array = datachunk.splitlines()
			linesPerPiece = len(data_array)/pieces
			log.debug(host_name + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': We are going to split taken logs at ' + str(pieces) + ' parts - ' + str(linesPerPiece) + ' lines in every part.')

			for index in range (0, pieces):
				l = []
				data = data_array[index*linesPerPiece:(index+1)*linesPerPiece]
				for i in data:
					l.append(globals()['PARSERS'][log_format](i))
				coll.insert(l, continue_on_error=True)
				log.debug(host_name + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': ' + str(len(l)) + ' lines have been parsed and saved to local MongoDB from part ' + str(index) + '.' )
				if l[-1]['Time'] > int(current_time):
					log.debug(host_name + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': Breaking parsing of logs at part ' + str(index) + ', because we have already reached ' + current_time + ' timestamp.')
					break
		else:
			data = globals()['PARSERS'][log_format](datachunk)
			data['Time'] = int(current_time)-(int(current_time)-int(previous_time))/2
			coll.insert(data)
			log.debug(host_name + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': Data has been parsed and saved to local MongoDB.' )

		indexes = config['index']
		log.debug(host_name + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': We are going to create next indexes: ' + str(indexes) + '.')
		for index in indexes:
			coll.ensure_index(index)
			log.debug(host_name + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': ' + str(index) + ' index has been successfully (re-)created.')
		connection.disconnect()
		log.debug(host_name + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': Connection to local MongoDB is closed.')

		status = 'success'
		log.info(host_name + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': Parsing has been successfully completed. Starting host aggregation.')
		# Сразу дёргаем агрегацию по этому хосту
		status, err = aggregate_host_str(host_name, config_name, group_name, previous_time, current_time)
		if err != None:
			return 'failed', str(err)

		finish_time = int(time.time())
		log.info(host_name + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': Parsing and aggregation of host has been successfully completed in ' + str(finish_time-start_time) + ' seconds.')
		return status,  None

	except Exception as err:
		status = 'failed'
		log.error(host_name + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': ' + str(err) + ' while parsing and aggregating data.', exc_info=1)
		return status, str(err)


def aggregate_host_str(host, config_name, group_name, previous_time, current_time):
	try:
		log = logging.getLogger(config['logger_name'])
		start_time = int(time.time())

		connection_local = common.mongo_connect_local()
		if connection_local is None:
			return 'failed', 'Could not connect to local MongoDB'
		log.debug(host + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': Connection to local MongoDB is established.')
		connection_remote = common.mongo_connect_replicaset(config['mongo_hosts'])
		if connection_remote is None:
			return 'failed', 'Could not connect to replica set'
		log.debug(host + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': Connection to replica set MongoDB is established.')
		db_local = connection_local[config['db_prefix'] + config_name + '_' + group_name]
		log.debug(host + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': We are going to use ' + db_local.name + ' local DB.')
		db_remote = connection_remote[config['db_prefix']+'mid']
		log.debug(host + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': We are going to use ' + db_remote.name + ' replica set DB.')
		host = host.replace('\n','').replace('-','_').replace('.','_')
		if host not in db_local.collection_names():
			log.error(host + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': Unknown host ' + host)
			return 'failed', 'No such collection in mongo ' + host

		limit_time = start_time - config['behind_host_time']
		res = db_remote[log_format_converted]
		log.debug(host + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': We are going to use ' + res.full_name + ' replica set collection.')
		coll = db_local[host]
		log.debug(host + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': We are going to use ' + coll.full_name + ' local collection.')
		host_log_time = int(current_time)
		if host_log_time > limit_time:
			host_log_time = limit_time
		log.debug(host + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': We are going to aggregate data from ' + previous_time + ' to ' + str(host_log_time) + ' seconds.')
		agg_config = common.generate_aggregation_config(config['agg_configs'])
		if agg_config is None or len(agg_config) == 0:
			log.error(host + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': Could not get or read aggregation config.')
			return 'failed', 'Could not get or parse aggregation config'
		log.debug(host + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': Aggregation config is ' + str(agg_config) + '.')

		# Формируем список того, что нужно сагрегировать
		todo = []
		for k, v in agg_config.items():
			for key in v['data'].keys():
				todo.append(k+':'+key)
		log.debug(host + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': Todo list is ' + str(todo) + '.')

		data = {} # Словарь, куда будем писать посчитанные значения
		for i in range(int(previous_time), host_log_time):
			log.debug(host + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': Aggregating data for ' + str(i) + ' second.')
			data[i] = {}
			for j in todo:
				graph, rate = j.split(':')
				# Заменяем %% на время
				p = re.compile('%%')
				value = p.sub('i', agg_config[graph]['data'][rate]['host'])
				if agg_config[graph]['data'][rate]['group'] == 'summa' or agg_config[graph]['data'][rate]['group'] == 'average':
					try:
						data[i][rate] = eval('coll' + value)
					except IndexError as err:
						data[i][rate] = 0
					if data[i][rate] == '' or data[i][rate] is None:
						data[i][rate] = 0
				elif agg_config[graph]['data'][rate]['group'] == 'quant':
					try:
						select = eval('coll' + value)
					except IndexError as err:
						continue
					for stroka in select:
						try:
							data[i][rate].append(stroka[agg_config[graph]['data'][rate]['field']])
						except KeyError:
							data[i][rate] = [stroka[agg_config[graph]['data'][rate]['field']],]
				else:
					log.error(host + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': We do not know such type of aggregation - ' + str(agg_config[graph]['data'][rate]['group']) + '.')
			log.debug(host + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': Aggregated data for ' + str(i) + ' second is ' + str(data[i]))
		# Складываем в mongo
		data_list = []
		for key, value in data.items():
			data_list.append({'_id': hashlib.sha256(host+';'+str(key)).hexdigest(), 'host': host, 'time': key, 'data': value})
		res.insert(data_list, continue_on_error=True)
		log.debug(host + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': Agregated data has been successfully saved to replica set DB.')

		connection_local.disconnect()
		log.debug(host + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': Connection to local MongoDB is closed.')
		connection_remote.disconnect()
		log.debug(host + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': Connection to replica set MongoDB is closed.')
		status = 'success'
		return status, None

	except Exception as err:
		log.error(host + ':' + config_name + ':' + group_name + ':' + previous_time + ':' + current_time + ': ' + str(err) + ' while aggregation data for host.', exc_info=1)
		status = 'failed'
		return status, str(err)


def parsing(io):
	""" Wrapper. This is hand for dealer"""
	hostname, config_name, group_name, previous_time, current_time = io.read().rstrip().split(';')
	status, ret = statbox_main(hostname, config_name, group_name, previous_time, current_time)
	if ret is None:
		answer = ';'.join((status, hostname, config_name, group_name, previous_time, current_time, socket.gethostname()))
	else:
		answer = ';'.join((status, hostname, config_name, group_name, previous_time, current_time, socket.gethostname(), ret))
	io.write(answer)


if __name__ == "__main__":
	hostname, config_name, group_name, previous_time, current_time = sys.argv[1].rstrip().split(';')
	statbox_main(hostname, config_name, group_name, previous_time, current_time)
