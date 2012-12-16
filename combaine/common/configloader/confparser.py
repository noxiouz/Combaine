#
# SO BAD CODE!!!!!!!!!!!!!!!!!!
#

import json
import pprint

class BaseConf(object):

    def load_conf(self, path):
        self.data = {}
        try:
            self.data = json.load(open(path, 'r'))
        except Exception as err:
            print str(err)
            return False
        else:
            return True

class CombaineConf(BaseConf):

    def get_lockserver(self): #Implement later
        raise NotImplementedError

    def get_local_db(self):
        try:
            ret =  self.data['cloud_config']['local_mongo_port']
        except Exception as err:
            print str(err)
        return ret

    def get_distributed_storage_conf(self):
        try:
            ret = self.data['cloud_config']["DistributedStorage"]
        except Exception as err:
            print str(err)
        else:
            return ret


    def get_data_fetcher_conf(self):
        try:
            ret = self.data['cloud_config']["DataFetcher"]
        except Exception as err:
            print str(err)
        else:
            return ret

    def get_local_data_base_conf(self):
        try:
            ret = self.data['cloud_config']["LocalDatabase"]
            pprint.pprint(ret)
        except Exception as err:
            print str(err)
        else:
            return ret

class ParsingConf(BaseConf):

    def get_aggregate_confs(self):
        print "Not implemented"
        raise Exception

    def get_data_fetcher(self):
        try:
            print self.data
            ret = self.data["DataFetcher"]
            pprint.pprint(ret)
        except Exception as err:
            print str(err)
        else:
            return ret


