from combaine.common.configloader import parse_agg_cfg
from combaine.common.configloader import parse_parsing_cfg
from combaine.common.configloader import parse_common_cfg

from combaine.common.loggers import CommonLogger


class ParsingConfigurator(object):

    def __init__(self, parsingconf, aggregation_config=None):
        self.logger = CommonLogger()
        self.logger.debug("read combaine config")
        self.metahost = None
        try:
            _combaine = parse_common_cfg("combaine")
            _parsing = parse_parsing_cfg(parsingconf)
            if aggregation_config is None:
                _aggregations = [(parse_agg_cfg(agg_name), agg_name) for agg_name in _parsing["agg_configs"]]
            else:
                _aggregations = [(parse_agg_cfg(aggregation_config), aggregation_config), ]
                self.metahost = _aggregations[0][0].get('metahost') or _parsing.get('metahost')
            self.ds = _combaine["cloud_config"]["DistributedStorage"]
            self.df = _combaine["cloud_config"]["DataFetcher"]
            self.db = _combaine["cloud_config"]["LocalDatabase"]
            _ds = _parsing.get("DistributedStorage")
            _df = _parsing.get("DataFetcher")
            _db = _parsing.get("LocalDatabase")
            self.hosts_fetcher_http_hand = _combaine['Combainer'].get('HTTP_HAND')
            self.parser = _parsing.get("parser")
            if not _ds is None:
                self.logger.debug("Update ds from parsing")
                self.ds.update(_ds)
            if not _df is None:
                self.logger.debug("Update ds from parsing")
                self.df.update(_df)
            if not _db is None:
                self.logger.debug("Update ds from parsing")
                self.db.update(_db)
            #===============
            agg_bind = {
                "summa" : "AverageAggregator",
                "quant" : "QuantAggregator",
                "average" : "AverageAggregator",
                "uniq" : "UniqAggregator",
            }
            self.aggregators = []
            self.resulthadlers = list()
            self.senders = list()
            for aggregator, _agg_name in _aggregations:
                for name, dic in aggregator["data"].iteritems():
                    tmp = dict()
                    tmp["name"] = _agg_name + "@" + name
                    tmp["query"] = dic.get("query", "EMPTY")
                    tmp["type"] = dic["type"]
                    tmp.update(dic)
                    if dic["type"] == "quant":
                        tmp["values"] = dic["values"]

                    tmp["type"] = agg_bind.get(dic["type"])  #DIRTY  HOOK!!!!!!!
                    if not tmp["type"] is None:
                        self.aggregators.append(tmp)

                # ResultHandlers configs - now in data
                for name, dic in aggregator["data"].iteritems():
                    #dic['type'] = name
                    dic['parsing_conf'] = _parsing
                    self.resulthadlers.append(dic)

                for name, dic in aggregator.get("senders", {}).iteritems():
                    dic['parsing_conf'] = _parsing
                    self.senders.append(dic)
        except Exception as err:
            self.logger.exception("Error in read confing")
            raise
