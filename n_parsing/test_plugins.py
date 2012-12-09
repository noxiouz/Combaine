#! /usr/bin/env python


config = {  'type'  : 'MongoReplicaSet',
            "hosts" : ["cocaine-mongo01g.kit.yandex.net:27017", "cocaine-mongo02g.kit.yandex.net:27017", "cocaine-mongo03f.kit.yandex.net:27017"]
}


from DistributedStorage import DistributedStorageFactory


print DistributedStorageFactory(**config)
