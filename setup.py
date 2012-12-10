try:
    from setuptools import setup, Extension
except ImportError:
    from distutils.core import setup, Extension



setup(
        name = "Combaine",
        version = "0.5.0",
        author = "noxiouz",
        author_email = "noxiouz@yandex.ru",
        description = "Some escrip",
        url = "https://github.com/noxiouz/Combaine",
        license = "GPL3",
        packages = [
            'combaine',
            'combaine.common',
            'combaine.combainer',
            'combaine.combainer.Scheduler',
            'combaine.combainer.Observer',
            'combaine.plugins.Aggregators',
            'combaine.plugins.DataGrid',
            'combaine.plugins.DistributedStorage',
            'combaine.plugins.DataFetcher',
            'combaine.plugins.StorageAPI',
            'combaine.plugins.LockServerAPI',
        ],
        data_files = [
            ('/usr/lib/yandex/combaine',['startCombainer.py']),
            ('/usr/lib/yandex/combaine/cocaine_deploy/aggregate',['cocaine_deploy/aggregate/__init__.py',
                                                                    'cocaine_deploy/aggregate/manifest-agg.json']),
            ('/usr/lib/yandex/combaine/cocaine_deploy/parsing',  ['cocaine_deploy/parsing/__init__.py',
                                                                    'cocaine_deploy/parsing/manifest-parsing.json']),
            ('/usr/lib/yandex/combaine/cocaine_deploy/recurring',['cocaine_deploy/recurring/__init__.py',
                                                                    'cocaine_deploy/recurring/manifest-recurring.json']),
            ('/usr/lib/yandex/combaine/cocaine_deploy/combainer',['cocaine_deploy/combainer/__init__.py',
                                                                    'cocaine_deploy/combainer/manifest-combainer.json']),
            ('/usr/lib/yandex/combaine/cocaine_deploy/',['./scripts/start_me_to_deploy.sh']),
            ('/etc/init.d/',['./scripts/combaine-machine']),
        ]
)

