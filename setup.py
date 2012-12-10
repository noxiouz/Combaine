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
        requires = [
            'python-zmq (>=2.1)', 
            'cocaine-framework-python (=0.9.5)',
            'cocaine-generic-slave (=0.9.3)',
            'cocaine-server (=0.9.3)',
            'cocaine-core1 (=0.9.3)'
            'cocaine-tools (=0.9.3)',
            'libcocaine-common1 (=0.9.3)',
            'libcocain-dealer1 (=0.9.4)',
            'libcocaine-plugin-chrono (=0.9.0)',
            'libcocaine-plugin-dealer (=0.9.0)',
            'libcocaine-plugin-python (=0.9.0)',
        ]
)
