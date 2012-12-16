import json
from pprint import pprint


class ParsingConfigurator(object):

    def __init__(self, parsingconf):
        # read combaine.json
        try:
            _combaine = json.load(open('/etc/combaine/combaine.json'))
            pprint(_combaine)
            _parsing = json.load(open('/etc/combaine/parsing/%s.json' % parsingconf))
            pprint(_parsing)
            _aggregations = [json.load(open('/etc/combaine/aggregate/%s.json' % agg_name)) for agg_name in _parsing["agg_configs"]]
            pprint(_aggregations)
            self.ds = _combaine["cloud_config"]["DistributedStorage"]
            self.df = _combaine["cloud_config"]["DataFetcher"]
            self.db = _combaine["cloud_config"]["LocalDatabase"]
            _ds = _parsing.get("DistributedStorage")
            _df = _parsing.get("DataFetcher")
            _db = _parsing.get("LocalDatabase")
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
                #"quant" : "NotImplemented",
                "average" : "AverageAggregator",
            }
            self.aggregators = []
            for aggregator in _aggregations:
                for name, dic in aggregator["data"].iteritems():
                    tmp = dict()
                    tmp["name"] = name
                    tmp["host"] = dic["host"]
                    tmp["group"] = dic["group"]
                    tmp["type"] = agg_bind.get(dic["group"])  #DIRTY  HOOK!!!!!!!
                    pprint(tmp)
                    if not tmp["type"] is None:
                        print "AAAAA"
                        self.aggregators.append(tmp)
            print "============== RESULT ================="
            pprint(self.ds)
            pprint(self.df)
            pprint(self.db)
            print "======================================="
            pprint(self.aggregators)
        except Exception as err:
            print str(err)
        

if __name__ == "__main__":
    ParsingConfigurator("feeds_nginx")
