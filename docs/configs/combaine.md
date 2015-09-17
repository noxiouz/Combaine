## Combainer config

Combainer config ( **combaine.yaml** ) contains several sections. Logically it's a mixture of different components.

```yaml
Combainer:
  Lockserver:
    app_id: Combaine
    host: ['zookeeper1.host.net:2181', 'zookeeper2.host.net:2181']
    name: combainer_lock
    timeout: 5
  Main:
    MINIMUM_PERIOD: 20
    cloud: combaine-cloud
    Cache:
      type: "InMemory"
cloud_config:
  DataFetcher:
    type: timetail
  	logname: nginx/access.log
  	timetail_port: 3132
  	timetail_url: '/timetail?log=',
  HostFetcher:
    type: predefine,
    Clusters: {
      "combaine-cloud": {
        "GROUP1": ["host1-IVA", "host2-IVA"],
        "GROUP2": ["host1-ugr", "localhost"]
      }
    }
```
