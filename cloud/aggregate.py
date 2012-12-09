# -*- coding: utf-8 -*-
# Да, я буду писать комментарии на русском.

import urllib, time, sys, socket, re, logging, hashlib
import common

def aggregate_group(io):
	""" Wrapper. This is hand for dealer"""
	groupname, config_name, agg_config_name, previous_time, current_time = io.read().rstrip().split(';')
	status, fail = aggregate_group_str(groupname, config_name, agg_config_name, previous_time, current_time)
	if fail is None:
		answer = ';'.join((status, groupname, config_name, agg_config_name, previous_time, current_time, socket.gethostname()))
	else:
		answer = ';'.join((status, groupname, config_name, agg_config_name, previous_time, current_time, socket.gethostname(), fail))
	io.write(answer)

# Именно здесь происходит магия агрегации
def aggregate_group_str(groupname, config_name, agg_config_name, previous_time, current_time):
	try:
		start_time = int(time.time())

		global config
		config = common.generate_parsing_config(config_name)
		if config is None or len(config) == 0:
			return 'failed', 'Could not get or parse config'

		log = logging.getLogger(config['logger_name'])
		log.debug(groupname + ':' + config_name + ':' + agg_config_name + ':' + previous_time + ':' + current_time + ': Started aggregation of group.')

		global connection
		log.debug(groupname + ':' + config_name + ':' + agg_config_name + ':' + previous_time + ':' + current_time + ': Replica set hosts are ' + str(config['mongo_hosts']) + '.')
		connection = common.mongo_connect_replicaset(config['mongo_hosts'])
		if connection is None:
			log.error(groupname + ':' + config_name + ':' + agg_config_name + ':' + previous_time + ':' + current_time + ': Could not connect to replica set.')
			return 'failed', 'Could not connect to replica set'

		global agg_config
		agg_config = common.generate_aggregation_config_single(agg_config_name)
		log.debug(groupname + ':' + config_name + ':' + agg_config_name + ':' + previous_time + ':' + current_time + ': Aggregation config is ' + str(agg_config) + '.')
		if agg_config is None or len(agg_config) == 0:
			log.error(groupname + ':' + config_name + ':' + agg_config_name + ':' + previous_time + ':' + current_time + ': Could not get or parse aggregation config.')
			return 'failed', 'Could not get or parse aggregation config'

		hosts = urllib.urlopen("http://" + config['conductor'] + "/groups2hosts/" + groupname + "?fields=root_datacenter_name,fqdn").read()
		if hosts == 'No groups found':
			log.error(groupname + ':' + config_name + ':' + agg_config_name + ':' + previous_time + ':' + current_time + ': Illegal group: ' + groupname)
			return 'failed', 'Illegal group'

		hosts_info = {groupname: []}
		for i in hosts.splitlines():
			dc, host = i.split('\t')
			host = host.replace('.','_').replace('-','_')
			try:
				hosts_info[groupname].append(host)
				hosts_info[groupname + '-' + dc].append(host)
			except KeyError:
				hosts_info[groupname + '-' + dc] = [host,]
		log.debug(groupname + ':' + config_name + ':' + agg_config_name + ':' + previous_time + ':' + current_time + ': Hosts_info is ' + str(hosts_info))

		db_mid = connection[config['db_prefix']+'mid']
		db_res = connection[config['db_prefix']+'res']
		log.debug(groupname + ':' + config_name + ':' + agg_config_name + ':' + previous_time + ':' + current_time + ': We are going to use next database names: ' + str(db_mid.name) + ', ' + str(db_res.name) + '.')
		log_format_converted = config['parser'].replace('-','_').replace('.','_')
		mid = db_mid[log_format_converted]
		log.debug(groupname + ':' + config_name + ':' + agg_config_name + ':' + previous_time + ':' + current_time + ': We are going to use next collection name: ' + str(mid.full_name) + '.')

		mid.ensure_index('host')
		mid.ensure_index('time')
		log.debug(groupname + ':' + config_name + ':' + agg_config_name + ':' + previous_time + ':' + current_time + ': Indexes by fields "host" and "time" have been checked/created.')


		limit_time = start_time - config['behind_group_time']
		group_log_time = int(current_time)
		if group_log_time > limit_time:
			group_log_time = limit_time
		log.debug(groupname + ':' + config_name + ':' + agg_config_name + ':' + previous_time + ':' + current_time + ': We are going to aggregate data from ' + str(previous_time) + ' to ' + str(group_log_time) + ' seconds.')

		itog = {}
		# Для каждого графика из конфига агрегации
		for graph in agg_config.keys():
			if agg_config[graph]['near_realtime'] == 'yes':
				log.debug(groupname + ':' + config_name + ':' + agg_config_name + ':' + previous_time + ':' + current_time + ': Starting per-second aggregation of data for graph ' + graph + '.')
				itog[graph + '_rt'] = []
				# Посекундная агрегация данных
				for group, hosts in hosts_info.items():
					for sec in range(int(previous_time), group_log_time):
						one_sec_cursor = mid.find({'time': sec, 'host': {'$in': hosts}})
						agg_group_result = {'_id': hashlib.sha256(group+';'+str(sec)).hexdigest(), 'time': sec, 'group': group, 'data': {}}
						for rate in agg_config[graph]['data'].keys():
							one_sec_data = one_sec_cursor.clone()
							if agg_config[graph]['data'][rate]['group'] == 'summa' or agg_config[graph]['data'][rate]['group'] == 'average':
								field = agg_config[graph]['data'][rate]['graph_field']
								for line in one_sec_data:
									try:
										agg_group_result['data'][field] += line['data'][rate]
									except KeyError:
										if line['data'].has_key(rate):
											agg_group_result['data'][field] = line['data'][rate]
										else:
											continue
								if len(hosts) and agg_config[graph]['data'][rate]['group'] == 'average':
									agg_group_result['data'][field] = agg_group_result['data'][field]/len(hosts)
							elif agg_config[graph]['data'][rate]['group'] == 'quant':
								result_list = []
								for line in one_sec_data:
									try:
										result_list += line['data'][rate]
									except KeyError:
										continue
								result_list.sort()
								maximum = len(result_list)
								if maximum == 0:
									continue
								for j in range(0, len(agg_config[graph]['data'][rate]['values'])):
									field = agg_config[graph]['data'][rate]['graph_fields'][j]
									value = agg_config[graph]['data'][rate]['values'][j]
									agg_group_result['data'][field] = result_list[maximum*value/100]
							if agg_group_result['data'].has_key(field) and isinstance(agg_group_result['data'][field], float):
								agg_group_result['data'][field] = round(agg_group_result['data'][field], 3)
						itog[graph + '_rt'].append(agg_group_result)
				log.debug(groupname + ':' + config_name + ':' + agg_config_name + ':' + previous_time + ':' + current_time + ': Result of per-second aggregation of data for graph ' + str(graph) + ' is ' + str(itog[graph + '_rt']) + '.')

			log.debug(groupname + ':' + config_name + ':' + agg_config_name + ':' + previous_time + ':' + current_time + ': Starting interval aggregation of data for graph ' + str(graph) + '.')
			itog[graph + '_interval'] = []
			period = agg_config[graph]['interval_period']
			log.debug(groupname + ':' + config_name + ':' + agg_config_name + ':' + previous_time + ':' + current_time + ': We are going to aggregate data for ' + str(period) + ' seconds.')
			if period == 1: # Ура!
				itog[graph + '_interval'] = itog[graph + '_rt']
			elif period == 0:
				raise UserWarning
			else:
				res = db_res[graph + '_interval']
				try:
					last_time = int(res.find({'group': groupname}).sort('time', -1).limit(1)[0]['time'])
					if int(current_time) - last_time < period:
						raise UserWarning
					log.debug(groupname + ':' + config_name + ':' + agg_config_name + ':' + previous_time + ':' + current_time + ': We are going to aggregate data for graph ' + str(graph) + ' from ' + str(last_time) + ' to ' + str(last_time + period) + '.')
					
					# Поинтервальная агрегация данных
					for group, hosts in hosts_info.items():
						all_time_cursor = mid.find({'time': {'$gte': last_time, '$lt': last_time + period}, 'host': {'$in': hosts}})
#						real_period = len(all_time_cursor.distinct('time'))
						agg_group_result = {'_id': hashlib.sha256(group+';'+str(last_time + period)).hexdigest(), 'time': last_time + period, 'group': group, 'data': {}}
						for rate in agg_config[graph]['data'].keys():
							all_time_data = all_time_cursor.clone()
							if agg_config[graph]['data'][rate]['group'] == 'summa' or agg_config[graph]['data'][rate]['group'] == 'average':
								field = agg_config[graph]['data'][rate]['graph_field']
								for line in all_time_data:
									try:
										agg_group_result['data'][field] += line['data'][rate]
									except KeyError:
										if line['data'].has_key(rate):
											agg_group_result['data'][field] = line['data'][rate]
										else:
											continue
								try:
									agg_group_result['data'][field] = agg_group_result['data'][field] / period
#									if real_period:
#										agg_group_result['data'][field] = agg_group_result['data'][field] / real_period
									if len(hosts) and agg_config[graph]['data'][rate]['group'] == 'average':
										agg_group_result['data'][field] = agg_group_result['data'][field] / len(hosts)
								except KeyError:
									pass
							elif agg_config[graph]['data'][rate]['group'] == 'quant':
								result_list = []
								for line in all_time_data:
									try:
										result_list += line['data'][rate]
									except KeyError:
										continue
								result_list.sort()
								maximum = len(result_list)
								if maximum == 0:
									continue
								for j in range(0, len(agg_config[graph]['data'][rate]['values'])):
									field = agg_config[graph]['data'][rate]['graph_fields'][j]
									value = agg_config[graph]['data'][rate]['values'][j]
									agg_group_result['data'][field] = result_list[maximum*value/100]
							if agg_group_result['data'].has_key(field) and isinstance(agg_group_result['data'][field], float):
								agg_group_result['data'][field] = round(agg_group_result['data'][field], 3)
						itog[graph + '_interval'].append(agg_group_result)
					log.debug(groupname + ':' + config_name + ':' + agg_config_name + ':' + previous_time + ':' + current_time + ': Result of interval aggregation of data for graph ' + str(graph) + ' is ' + str(itog[graph + '_interval']) + '.')
				except IndexError:
					res.insert({'time': int(current_time), 'group': groupname})
					last_time = 0
				except UserWarning:
					pass

		for coll_name, data_list in itog.items():
			res = db_res[coll_name]
			if len(data_list) > 0:
				res.insert(data_list, continue_on_error=True)
		log.debug(groupname + ':' + config_name + ':' + agg_config_name + ':' + previous_time + ':' + current_time + ': Successfully saved data to replica set.')

		status = 'success'
		# Складывание данных в spool для последующей отправки agave
		agave_spool(groupname, last_time, config_name, previous_time, current_time)
		# Отправка данных в razladki
		razladki_send(groupname, last_time, config_name, previous_time, current_time)
		finish_time = int(time.time())
		log.info(groupname + ':' + config_name + ':' + agg_config_name + ':' + previous_time + ':' + current_time + ': Aggregation has been completed successfully in ' + str(finish_time-start_time) + ' seconds.')
		return status, None
	except Exception as err:
		log.error(groupname + ':' + config_name + ':' + agg_config_name + ':' + previous_time + ':' + current_time + ': Aggregation of group data failed.', exc_info=1)
		status = 'failed'
		return status, str(err)

# Складывание данных в spool перед отправкой agave
def agave_spool(group, last_time, config_name, previous_time, current_time):
	try:
		log = logging.getLogger(config['logger_name'])
		db = connection[config['db_prefix']+'res']

		graph_names = agg_config.keys()
		log.debug(group + ':' + config_name + ':' + previous_time + ':' + current_time + ': We are going to put data for graphs ' + str(graph_names) + ' to agave spool.')
		db2 = connection['agave_spool']
		# Для каждого графика
		for graph in graph_names:
			coll = db[graph + '_interval']
			coll.ensure_index('group')
			coll.ensure_index('time')
			l = []
			# Для каждой строчки в коллекции
			for doc in coll.find({'group': {'$regex': '^'+group}, 'time': {'$gt': last_time}}):
				_temp_ = ''
				# Для каждого параметра этого графика
				for key in agg_config[graph]['data'].keys():
					if agg_config[graph]['data'][key]['group'] == 'summa' or agg_config[graph]['data'][key]['group'] == 'average':
						try:
							_t1_ = agg_config[graph]['data'][key]['graph_field']
							_t2_ = doc['data'][_t1_]
							_temp_ += str(_t1_) + ':' + str(_t2_) + "+"
						except KeyError as err:
							pass
					elif agg_config[graph]['data'][key]['group'] == 'quant':
						for i in range(0, len(agg_config[graph]['data'][key]['graph_fields'])):
							try:
								_t1_ = agg_config[graph]['data'][key]['graph_fields'][i]
								_t2_ = doc['data'][_t1_]
								_temp_ += str(_t1_) + ":" + str(_t2_) + "+"
							except KeyError:
								pass
				# Убираем последний плюс
				_temp_ = _temp_[:-1]
				if len(_temp_) > 0:
					l.append({'time': doc['time'], 'url': "/api/update/"+doc['group']+"/"+graph+"?values="+str(_temp_)+"&ts="+str(doc['time'])+"&template="+str(agg_config[graph]['graph_template'])+"&title="+graph})
			log.debug(group + ':' + config_name + ':' + previous_time + ':' + current_time + ': Dots for graph ' + str(graph) + ' are ' + str(l) + '.')
			coll2 = db2[graph.replace('-','_').replace('.','_')]
			if len(l) > 0:
				coll2.insert(l)
			log.debug(group + ':' + config_name + ':' + previous_time + ':' + current_time + ': Dots for graph ' + str(graph) + ' have been saved to ' + str(coll2.full_name) + ' collection.')
		log.info(group + ':' + config_name + ':' + previous_time + ':' + current_time + ': Data has been saved to agave spool successfully.')
		return 0
	except Exception as err:
		log.error(group + ':' + config_name + ':' + previous_time + ':' + current_time + ': Saving data to agave spool failed.', exc_info=1)
		return 1

# Отправка данных в razladki
def razladki_send(group, last_time, config_name, previous_time, current_time):
	try:
		log = logging.getLogger(config['logger_name'])

		graph_names = agg_config.keys()
		log.debug(group + ':' + config_name + ':' + previous_time + ':' + current_time + ': We are going to send data for graphs ' + str(graph_names) + ' to razladki.')
		db = connection[config['db_prefix']+'res']

		for graph in graph_names:
			coll = db[graph + '_interval']
			razladki_project = config['razladki_project']
			log.debug(group + ':' + config_name + ':' + previous_time + ':' + current_time + ':' + graph + ':' + ' Razladki project is ' + str(razladki_project) + '.')
			for doc in coll.find({'group': {'$regex': '^'+group}, 'time': {'$gt': last_time}}):
				for key in agg_config[graph]['data'].keys():
					if (agg_config[graph]['data'][key]['group'] == 'summa' or agg_config[graph]['data'][key]['group'] == 'average') and agg_config[graph]['data'][key]['send_razladki'] == 1:
						_temp_ = agg_config[graph]['data'][key]['graph_field']
						url = "http://" + config['razladki_host'] + "/save_new_datapoint/" + razladki_project + "?param=" + str(doc['group']) + '-' + graph + '_' + str(_temp_)  + '&value=' + str(doc['data'][_temp_]) + '&host_group=' + str(doc['group']) + '&ts=' + str(doc['time'])
						resp = urllib.urlopen(url).read()
						if resp.rstrip() == 'ok' or resp.rstrip() == 'Attempts to override existind data were ignored':
							log.debug(group + ':' + config_name + ':' + previous_time + ':' + current_time + ':' + graph + ':' + ' URL is ' + str(url) + ' and response is ' + str(resp.rstrip()) + '.')
							continue
						else:
							log.error(group + ':' + config_name + ':' + previous_time + ':' + current_time + ':' + graph + ':' + ' URL is ' + str(url) + ' and response is ' + str(resp.rstrip()) + '.')
					elif agg_config[graph]['data'][key]['group'] == 'quant' and len(agg_config[graph]['data'][key]['razladki_fields']) != 0:
						_temp_values_ = ''
						for i in range(0, len(agg_config[graph]['data'][key]['razladki_fields'])):
							_temp_ = agg_config[graph]['data'][key]['razladki_fields'][i]
							url = "http://" + config['razladki_host'] + "/save_new_datapoint/" + razladki_project + "?param=" + str(doc['group']) + '-' + graph + '_' + str(_temp_)  + '&value=' + str(doc['data'][_temp_]) + '&host_group=' + str(doc['group']) + '&ts=' + str(doc['time'])
							resp = urllib.urlopen(url).read()
							if resp.rstrip() == 'ok' or resp.rstrip() == 'Attempts to override existind data were ignored':
								log.debug(group + ':' + config_name + ':' + previous_time + ':' + current_time + ':' + graph + ':' + ' URL is ' + str(url) + ' and response is ' + str(resp.rstrip()) + '.')
								continue
							else:
								log.error(group + ':' + config_name + ':' + previous_time + ':' + current_time + ':' + graph + ':' + ' URL is ' + str(url) + ' and response is ' + str(resp.rstrip()) + '.')

		connection.disconnect()
		log.debug(group + ':' + config_name + ':' + previous_time + ':' + current_time + ': ' + 'Connection to replica set is closed.')
	except KeyError:
		pass
	except Exception as err:
		log.error(group + ':' + config_name + ':' + previous_time + ':' + current_time + ': ' + str(err) + ' while trying to send data to razladki.', exc_info=1)
		connection.disconnect()
		return 1



#==============================================================================
#====================================MAIN======================================
#==============================================================================
if __name__ == "__main__":
	groupname, config_name, previous_time, current_time = sys.argv[1].split(';')
	aggregate_group_str(groupname, config_name, previous_time, current_time)


