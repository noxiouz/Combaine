try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

_version = "0.5.2"

setup(
        name = "Combaine",
        version = _version,
        author = "noxiouz",
        author_email = "noxiouz@yandex.ru",
        description = "Distributed fault-tolerant system of data processing based on Cocaine (https://github.com/cocaine)",
        url = "https://github.com/noxiouz/Combaine",
        license = "GPL3",
        packages = [
            'combaine',
            'combaine.common',
            'combaine.common.configloader',
            'combaine.common.ZKeeperAPI',
            'combaine.common.interfaces',
            'combaine.cloud',
            'combaine.combainer',
            'combaine.combainer.Scheduler',
            'combaine.combainer.Observer',
            'combaine.plugins',
            'combaine.plugins.Aggregators',
            'combaine.plugins.DataGrid',
            'combaine.plugins.DistributedStorage',
            'combaine.plugins.DataFetcher',
            'combaine.plugins.StorageAPI',
            'combaine.plugins.LockServerAPI',
            'combaine.plugins.Senders',
            'combaine.plugins.ResultHandler',
        ],
        data_files = [
            ('/usr/lib/yandex/combaine',['startCombainer.py']),
            ('/usr/lib/yandex/combaine/cocaine_deploy/aggregate',['cocaine_deploy/aggregate/__init__.py',
                                                                    'cocaine_deploy/aggregate/manifest-agg.json']),
            ('/usr/lib/yandex/combaine/cocaine_deploy/parsing',  ['cocaine_deploy/parsing/__init__.py',
                                                                    'cocaine_deploy/parsing/manifest-parsing.json']),
            ('/usr/lib/yandex/combaine/cocaine_deploy/combainer',['cocaine_deploy/combainer/__init__.py',
                                                                    'cocaine_deploy/combainer/manifest-combainer.json']),
            ('/usr/sbin/',['./scripts/combaine-deploy']),
            ('/usr/sbin/',['./utils/combaine-check-conf']),
            ('/usr/sbin/',['./utils/combaine-find-parser']),
            ('/usr/sbin/',['./utils/combaine-check-work']),
            ('/usr/sbin/',['./utils/convert_51to52']),
            ('/etc/init.d/',['./scripts/combaine-machine']),
        ],
        requires = ['yaml', 'zookeeper', 'pymongo']
)

