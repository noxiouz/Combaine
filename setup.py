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
        py_modules = [
            'startCombainer'
        ]
)
