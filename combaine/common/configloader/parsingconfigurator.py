import json
from pprint import pprint


class ParsingConfigurator(object):

    def __init__(self, parsingconf, aggregation_config=None):
        # read combaine.json
        try:
            _combaine = json.load(open('/etc/combaine/combaine.json'))
            #pprint(_combaine)
            _parsing = json.load(open('/etc/combaine/parsing/%s.json' % parsingconf))
            #pprint(_parsing)
            if aggregation_config is None:
                _aggregations = [(json.load(open('/etc/combaine/aggregate/%s.json' % agg_name)), agg_name) for agg_name in _parsing["agg_configs"]]
            else:
                _aggregations = [(json.load(open('/etc/combaine/aggregate/%s.json' % aggregation_config)), aggregation_config), ]
            #pprint(_aggregations)
            self.ds = _combaine["cloud_config"]["DistributedStorage"]
            self.df = _combaine["cloud_config"]["DataFetcher"]
            self.db = _combaine["cloud_config"]["LocalDatabase"]
            _ds = _parsing.get("DistributedStorage")
            _df = _parsing.get("DataFetcher")
            _db = _parsing.get("LocalDatabase")
            self.hosts_fetcher_http_hand = _combaine['Combainer'].get('HTTP_HAND')
            self.parser = _parsing.get("parser")
            if not _ds is None:
                print "Update ds from parsing"
                self.ds.update(_ds)
            if not _df is None:
                print "Update ds from parsing"
                self.df.update(_df)
            if not _db is None:
                print "Update ds from parsing"
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
                    #pprint(tmp)
                    if not tmp["type"] is None:
                        self.aggregators.append(tmp)
                if aggregator.has_key("ResultHandlers"):
                    for name, dic in aggregator["ResultHandlers"].iteritems():
                        dic['type'] = name
                        self.resulthadlers.append(dic)

        except Exception as err:
            print "ERRROOOORRRISHE!!!" + str(err)
            raise
